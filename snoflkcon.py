import snowflake.connector
import dbcfg

def get_connection():
    print("Getting connection...")
    cnct = snowflake.connector.connect(
        user=dbcfg.snoflkcon["user"],
        password=dbcfg.snoflkcon["password"],
        account=dbcfg.snoflkcon["account"]
    )
    if cnct is None:
        print("Connection is None")
    curs = cnct.cursor()
    curs.execute("USE WAREHOUSE " + dbcfg.snoflkcon["warehouse"])
    curs.execute("USE DATABASE " + dbcfg.snoflkcon["database"])
    curs.execute("USE SCHEMA " + dbcfg.snoflkcon["schema"])

    return (cnct)

flnm2tbl = {
    "strategy_strategy_set_member_l": "strategy_x_strategy_set_member_l",
    "strategy_set_strategy_set_member_l": "strategy_set_x_strategy_set_member_l",
    "strategy_set_member_weighted_strategy_set_member_l": "strategy_set_member_x_weighted_strategy_set_member_l",
    "weighted_strategy_set_weighted_strategy_set_member_l": "weighted_strategy_set_x_weighted_strategy_set_member_l",
    "weighted_strategy_set_member_competition_grp_member_l": "weighted_strategy_set_member_x_competition_grp_member_l",
    # "competition_grp_member_competition_grp_l": "competition_grp_member_x_competition_grp_l",
    "competition_grp_competition_grp_member_l": "competition_grp_x_competition_grp_member_l",
    "strategy_set_weighted_strategy_set_l": "strategy_set_x_weighted_strategy_set_l",
    "weighted_strategy_set_agent_value_l": "weighted_strategy_set_x_agent_value_l",
    "weighted_strategy_set_game_results_l": "weighted_strategy_set_x_game_results_l",
    # "game_results_game_stats_l": "game_results_x_game_stats_l",
    "game_stats_game_results_l": "game_stats_x_game_results_l",
    "strategy_h": "strategy_h",
    "strategy_set_h": "strategy_set_h",
    "strategy_set_member_h": "strategy_set_member_h",
    "weighted_strategy_set_h": "weighted_strategy_set_h",
    "weighted_strategy_set_member_h": "weighted_strategy_set_member_h",
    "game_results_h": "game_results_h",
    "game_stats_h": "game_stats_h",
    "competition_grp_h": "competition_grp_h",
    "competition_grp_member_h": "competition_grp_member_h",
    "agent_value_h": "agent_value_h",
    "strategy_s": "strategy_s",
    "strategy_set_s": "strategy_set_s",
    "weighted_strategy_set_s": "weighted_strategy_set_s",
    "competition_grp_s": "competition_grp_s",
    "game_results_s": "game_results_s",
    "game_stats_s": "game_stats_s"
}
