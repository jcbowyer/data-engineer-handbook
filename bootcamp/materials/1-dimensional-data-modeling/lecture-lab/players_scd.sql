Drop table if exists players_scd;
create table players_scd
(
	player_name text,
	scoring_class scoring_class,
	is_active boolean,
	start_season integer,
	end_season integer,
	current_season INTEGER,
	Primary Key (	player_name, start_season)
);

