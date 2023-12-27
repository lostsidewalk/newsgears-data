package com.lostsidewalk.buffy.rule;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.lostsidewalk.buffy.DataAccessException;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayListWithCapacity;
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

    @Override
    public final String toString() {
        return "RuleSetDao{" +
                "jdbcTemplate=" + jdbcTemplate +
                ", RULE_SET_ROW_MAPPER=" + RULE_SET_ROW_MAPPER +
                '}';
    }
}
