package com.tesco.aqueduct.registry;

import com.fasterxml.jackson.databind.JavaType;
import com.tesco.aqueduct.pipe.api.JsonHelper;
import com.tesco.aqueduct.registry.model.Node;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class PostgreSQLNodeRegistry implements NodeRegistry {

    private final URL cloudUrl;
    private final Duration offlineDelta;
    private final DataSource dataSource;
    private static final int OPTIMISTIC_LOCKING_COOLDOWN = 5;
    private static final int NUMBER_OF_CHILDREN = 2;

    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(PostgreSQLNodeRegistry.class));

    public PostgreSQLNodeRegistry(DataSource dataSource, URL cloudUrl, Duration offlineDelta) {
        this.cloudUrl = cloudUrl;
        this.offlineDelta = offlineDelta;
        this.dataSource = dataSource;
    }

    @Override
    public List<URL> register(Node node) {
        while (true) {
            try (Connection connection = dataSource.getConnection()) {
                NodeGroup group = getNodeGroup(connection, node.getGroup());
                ZonedDateTime now = ZonedDateTime.now();

                if (group.isEmpty()) {
                    node = makeNodeFollowCloud(node, now);
                    group = new NodeGroup(node);
                    insertNewGroup(connection, group);
                } else {
                    Node existingNode = group.getById(node.getId());
                    if (existingNode != null) {
                        node = updateExistingNode(existingNode, node, group);
                    } else {
                        node = addNodeToExistingGroup(group, node, now);
                    }
                    persistGroup(connection, group);
                }
                return node.getRequestedToFollow();
            } catch (SQLException | IOException exception) {
                LOG.error("Postgresql node registry", "register node", exception);
                throw new RuntimeException(exception);
            } catch (VersionChangedException exception) {
                try {
                    Thread.sleep(OPTIMISTIC_LOCKING_COOLDOWN);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public StateSummary getSummary(long offset, String status, List<String> groups) {
        try (Connection connection = dataSource.getConnection()) {
            List<Node> followers;

            if (groups == null || groups.isEmpty()) {
                followers = getAllNodes(connection);
            } else {
                followers = getNodesFilteredByGroup(connection, groups);
            }

            followers = followers
                .stream()
                .map(this::changeStatusIfOffline)
                .collect(Collectors.toList());

            Node node = Node.builder()
                .localUrl(cloudUrl)
                .offset(offset)
                .status(status)
                .following(Collections.emptyList())
                .lastSeen(ZonedDateTime.now())
                .build();

            return new StateSummary(node, followers);
        } catch (SQLException exception) {
            LOG.error("Postgresql node registry", "get summary", exception);
            throw new RuntimeException(exception);
        }
    }

    @Override
    public boolean deleteNode(String groupId, String nodeId) {
        while (true) {
            try (Connection connection = dataSource.getConnection()) {
                NodeGroup nodeGroup = getNodeGroup(connection, groupId);

                if(nodeGroup.isEmpty()) {
                    return false;
                } else {
                    return deleteExistingNode(connection, groupId, nodeId, nodeGroup);
                }
            } catch (SQLException | IOException exception) {
                LOG.error("Postgresql node registry", "deleteNode", exception);
                throw new RuntimeException(exception);
            } catch (VersionChangedException exception) {
                try {
                    Thread.sleep(OPTIMISTIC_LOCKING_COOLDOWN);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private boolean deleteExistingNode(Connection connection, String groupId, String nodeId, NodeGroup group) throws IOException, SQLException {
        boolean foundNode = group.removeById(nodeId);

        if (foundNode) {
            if (group.isEmpty()) {
                deleteGroup(connection, group.version, groupId);
            } else {
                NodeGroup rebalancedGroup = rebalanceGroup(group);
                persistGroup(connection, rebalancedGroup);
            }
            return true;
        }
        return false;
    }

    private NodeGroup rebalanceGroup(NodeGroup group) {
        List<URL> allUrls = group.getNodeUrls();
        List<Node> rebalancedNodes = new ArrayList<>();

        for (int i = 0; i < allUrls.size(); i++) {
            List<URL> followUrls = getFollowerUrls(allUrls, i);

            Node updatedNode = group
                .get(i)
                .toBuilder()
                .requestedToFollow(followUrls)
                .build();

            rebalancedNodes.add(updatedNode);
        }
        return new NodeGroup(rebalancedNodes, group.version);
    }

    private void deleteGroup(Connection connection, int version, String groupId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(QUERY_DELETE_GROUP)) {

            statement.setString(1, groupId);
            statement.setInt(2, version);

            if (statement.executeUpdate() == 0) {
                throw new VersionChangedException();
            }
        }
    }

    private Node updateExistingNode(Node existingValue, Node newValues, NodeGroup group) {
        //create a new node, with the existing "requestedToFollow" values
        Node updatedNode = newValues.toBuilder()
                .requestedToFollow(existingValue.getRequestedToFollow())
                .lastSeen(ZonedDateTime.now())
                .build();
        return group.updateNode(updatedNode);
    }

    private Node makeNodeFollowCloud(final Node node, final ZonedDateTime now){
        List<URL> followUrls = Collections.singletonList(cloudUrl);
        return node.toBuilder()
            .requestedToFollow(followUrls)
            .lastSeen(now)
            .build();
    }

    private Node addNodeToExistingGroup(NodeGroup group, Node node, ZonedDateTime now) {
        List<URL> nodeUrls = group.getNodeUrls();

        int nodeIndex = nodeUrls.size();
        List<URL> followUrls = getFollowerUrls(nodeUrls, nodeIndex);

        Node newNode = node.toBuilder()
            .requestedToFollow(followUrls)
            .lastSeen(now)
            .build();

        group.add(newNode);

        return newNode;
    }

    private List<URL> getFollowerUrls(List<URL> allUrls, int nodeIndex) {
        List<URL> followUrls = new ArrayList<>();

        while (nodeIndex != 0) {
            nodeIndex = ((nodeIndex + 1) / NUMBER_OF_CHILDREN) - 1;
            followUrls.add(allUrls.get(nodeIndex));
        }

        followUrls.add(cloudUrl);
        return followUrls;
    }

    private Node changeStatusIfOffline(Node node) {
        ZonedDateTime threshold = ZonedDateTime.now().minus(offlineDelta);

        if (node.getLastSeen().compareTo(threshold) < 0) {
            return node.toBuilder().status("offline").build();
        }
        return node;
    }

    private List<Node> getNodesFilteredByGroup(Connection connection, List<String> groups) throws SQLException {
        List<Node> list = new ArrayList<>();
        for (String group : groups) {
            NodeGroup nodeGroup = getNodeGroup(connection, group);
            list.addAll(nodeGroup.nodes);
        }
        return list;
    }

    private NodeGroup getNodeGroup(Connection connection, String group) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(QUERY_GET_GROUP_BY_ID)) {
            statement.setString(1, group);

            try (ResultSet rs = statement.executeQuery()) {
                List<Node> nodes = new ArrayList<>();
                int version = 0;
                while (rs.next()) {
                    String entry = rs.getString("entry");
                    version = rs.getInt("version");

                    nodes.addAll(readGroupEntry(entry));
                }
                return new NodeGroup(nodes, version);
            } catch (IOException e) {
                e.printStackTrace();
                throw new UncheckedIOException(e);
            }
        }
    }

    private List<Node> readGroupEntry(String entry) throws IOException {
        JavaType type = JsonHelper.MAPPER.getTypeFactory().constructCollectionType(List.class, Node.class);
        return JsonHelper.MAPPER.readValue(entry, type);
    }

    private void persistGroup(Connection connection, NodeGroup group) throws SQLException, IOException {
        try (PreparedStatement statement = connection.prepareStatement(QUERY_UPDATE_GROUP)) {
            String jsonNodes = group.nodesToJson();

            statement.setString(1, jsonNodes);
            statement.setString(2, group.get(0).getGroup());
            statement.setInt(3, group.version);

            if (statement.executeUpdate() == 0) {
                throw new VersionChangedException();
            }
        }
    }

    private boolean insertNewGroup(Connection connection, NodeGroup group) throws IOException, SQLException {
        try (PreparedStatement statement = connection.prepareStatement(QUERY_INSERT_GROUP)) {
            String jsonNodes = group.nodesToJson();
            statement.setString(1, group.get(0).getGroup());
            statement.setString(2, jsonNodes);

            if (statement.executeUpdate() == 0) {
                //No rows updated
                throw new VersionChangedException();
            }
            return true;
        }
    }

    private List<Node> getAllNodes(Connection connection) throws SQLException {
        List<Node> nodes;
        try (PreparedStatement statement = connection.prepareStatement(QUERY_GET_ALL_NODES)) {
            nodes = new ArrayList<>();

            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    String entry = rs.getString("entry");
                    nodes.addAll(readGroupEntry(entry));
                }
            } catch (IOException e) {
                e.printStackTrace();
                throw new UncheckedIOException(e);
            }
        }

        return nodes;
    }

    private static final String QUERY_GET_GROUP_BY_ID = "SELECT entry, version FROM registry where group_id = ? ;";

    private static final String QUERY_UPDATE_GROUP =
        "UPDATE registry SET " +
            "entry = ?::JSON , " +
            "version = registry.version + 1 " +
        "WHERE " +
            "registry.group_id = ? " +
        "AND " +
            "registry.version = ? " +
        ";";

    private static final String QUERY_INSERT_GROUP =
        "INSERT INTO registry (group_id, entry, version)" +
        "VALUES (" +
            "?, " +
            "?::JSON, " +
            "0 " +
        ")" +
        "ON CONFLICT DO NOTHING ;";

    private static final String QUERY_GET_ALL_NODES = "SELECT entry FROM registry ORDER BY group_id;";
    private static final String QUERY_DELETE_GROUP = "DELETE from registry where group_id = ? and version = ? ;";
}
