package com.lostsidewalk.buffy.rule;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.lostsidewalk.buffy.DataAccessException;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static java.sql.Types.VARCHAR;
import static org.apache.commons.lang3.ObjectUtils.isNotEmpty;

/**
 * Data access object for managing rule sets in the application.
 */
@SuppressWarnings("OverlyBroadCatchBlock")
@Slf4j
@Component
public class RuleSetDao {

    private static final Gson GSON = new Gson();

    @SuppressWarnings({"EmptyClass", "AnonymousInnerClass"})
    private static final Type SET_RULE_TYPE = new TypeToken<Set<Rule>>() {}.getType();

    @Autowired
    JdbcTemplate jdbcTemplate;

    /**
     * Default constructor; initialize the object.
     */
    RuleSetDao() {
    }

    private final RowMapper<RuleSet> RULE_SET_ROW_MAPPER = (rs, rowNum) -> {
        Long id = rs.getLong("id");
        String username = rs.getString("username");
        String ruleSetName = rs.getString("rule_set_name");
        Set<Rule> rules = null;
        PGobject rulesObj = (PGobject) rs.getObject("rules");
        if (null != rulesObj) {
            rules = GSON.fromJson(rulesObj.getValue(), SET_RULE_TYPE);
        }

        return RuleSet.from(id, username, ruleSetName, rules);
    };

    private static final String FIND_QUEUE_RULE_SET_SQL = "select * from rule_set_definitions r " +
            " join queue_import_rule_sets qirs on qirs.rule_set_id = r.id " +
            " join subscription_definitions sd on sd.queue_id = qirs.queue_id " +
            " where sd.username = ? and sd.id = ?";

    private static final String FIND_SUBSCRIPTION_RULE_SET_SQL = "select * from rule_set_definitions r " +
            " join subscription_import_rule_sets sirs on sirs.rule_set_id = r.id " +
            " join subscription_definitions sd on sd.id = sirs.subscription_id " +
            " where sd.username = ? and sd.id = ?";

    /**
     * Retrieves a list of RuleSet objects by the specified username and subscriptionId.
     *
     * @param username The username associated with the rule set.
     * @param subscriptionId The identifier of the subscription.
     * @return A RuleSet object if found, otherwise null if not found.
     * @throws DataAccessException If a data access exception occurs.
     */
    @SuppressWarnings("unused")
    public final List<RuleSet> findBySubscriptionId(String username, Long subscriptionId) throws DataAccessException {
        List<RuleSet> ruleSets = newArrayListWithCapacity(2);
        try {
            List<RuleSet> queueRuleSetResults = jdbcTemplate.query(FIND_QUEUE_RULE_SET_SQL, RULE_SET_ROW_MAPPER, username, subscriptionId);
            if (isNotEmpty(queueRuleSetResults)) {
                ruleSets.add(queueRuleSetResults.get(0));
            }
            List<RuleSet> subscriptionRuleSetResults = jdbcTemplate.query(FIND_SUBSCRIPTION_RULE_SET_SQL, RULE_SET_ROW_MAPPER, username, subscriptionId);
            if (isNotEmpty(subscriptionRuleSetResults)) {
                ruleSets.add(subscriptionRuleSetResults.get(0));
            }
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findBySubscriptionId", e.getMessage(), username, subscriptionId);
        }
        return ruleSets;
    }

    private final ResultSetExtractor<Map<Long, RuleSet>> RULE_SET_MAPPING_EXTRACTOR = new ResultSetExtractor<>() {
        /**
         * @param rs the ResultSet to extract data from. Implementations should not close this: it will be closed by the calling JdbcTemplate
         * @return a mapping of entity Id -> RuleSet objects for rule sets of the specified type, owned by the specified username
         * @throws SQLException If a SQL error occurs.
         * @throws org.springframework.dao.DataAccessException If a data access exception occurs.
         */
        @Override
        public Map<Long, RuleSet> extractData(ResultSet rs) throws SQLException {
            Map<Long, RuleSet> resultMap = new HashMap<>();
            while (rs.next()) {
                Long entityId = rs.getLong("entity_id");
                Long id = rs.getLong("id");
                String username = rs.getString("username");
                String ruleSetName = rs.getString("rule_set_name");
                Set<Rule> rules = null;
                PGobject rulesObj = (PGobject) rs.getObject("rules");
                if (null != rulesObj) {
                    rules = GSON.fromJson(rulesObj.getValue(), SET_RULE_TYPE);
                }

                resultMap.put(entityId, RuleSet.from(id, username, ruleSetName, rules));

            }
            return resultMap;
        }
    };

    private static final String FIND_RULE_SET_BY_USERNAME_SQL_TEMPLATE = "select %s as entity_id,rs.* from %s t " +
            " join rule_set_definitions rs on rs.id = t.rule_set_id " +
            " where username = ?";

    /**
     * Rule set mapping type
     */
    public enum RuleSetMappingType {
        /**
         * Designates a rule set as a queue import rule set.
         */
        QUEUE_IMPORT("queue_id", "queue_import_rule_sets"),
        /**
         * Designates a rule set as a subscription import rule set.
         */
        SUBSCRIPTION_IMPORT("subscription_id", "subscription_import_rule_sets");

        final String entityIdColumn;

        final String tableName;

        RuleSetMappingType(String entityIdColumn, String tableName) {
            this.entityIdColumn = entityIdColumn;
            this.tableName = tableName;
        }

        @Override
        public String toString() {
            return "RuleSetMappingType{" +
                    "entityIdColumn='" + entityIdColumn + '\'' +
                    ", tableName='" + tableName + '\'' +
                    '}';
        }
    }

    /**
     * Retrieves a mapping of entity Id -> RuleSet objects for rule sets of the specified type, owned by the
     * specified username.
     *
     * @param username The username associated with the rule set.
     * @param ruleSetMappingType The type of rule set to fetch.
     * @return A RuleSet object if found, otherwise null if not found.
     * @throws DataAccessException If a data access exception occurs.
     */
    @SuppressWarnings("unused")
    public final Map<Long, RuleSet> findRuleSetByUsername(String username, RuleSetMappingType ruleSetMappingType) throws DataAccessException {
        try {
            String sql = String.format(FIND_RULE_SET_BY_USERNAME_SQL_TEMPLATE, ruleSetMappingType.entityIdColumn, ruleSetMappingType.tableName);
            return jdbcTemplate.query(sql, new Object[]{username}, new int[]{VARCHAR}, RULE_SET_MAPPING_EXTRACTOR);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findRuleSetByUsername", e.getMessage(), username, ruleSetMappingType);
        }
    }

    @Override
    public final String toString() {
        return "RuleSetDao{" +
                "jdbcTemplate=" + jdbcTemplate +
                ", RULE_SET_ROW_MAPPER=" + RULE_SET_ROW_MAPPER +
                ", RULE_SET_MAPPING_EXTRACTOR=" + RULE_SET_MAPPING_EXTRACTOR +
                '}';
    }
}
