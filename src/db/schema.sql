-- NBA Analytics Pipeline — schema for Supabase (Postgres)
-- Run once in the Supabase SQL Editor (or via psql) against a new project.
-- Matches the column names/types written by src/etl/load/*.py.

create schema if not exists core;

-- =========================================================
-- dim_teams
-- =========================================================
create table if not exists core.dim_teams (
    team_id       integer primary key,
    full_name     text,
    abbreviation  text,
    nickname      text,
    city          text,
    state         text,
    year_founded  integer,
    team_logo     text
);

-- =========================================================
-- dim_players
-- =========================================================
create table if not exists core.dim_players (
    id               integer primary key,
    full_name        text,
    first_name       text,
    last_name        text,
    is_active        boolean,
    player_headshot   text
);

-- =========================================================
-- dim_standings
-- =========================================================
create table if not exists core.dim_standings (
    season_team_id     text primary key,
    season_id          integer,
    team_id            integer,
    team_city          text,
    team_name          text,
    conference         text,
    conference_record  text,
    playoff_rank       integer,
    clinch_indicator   text,
    division           text,
    division_record    text,
    division_rank      integer,
    wins               integer,
    losses             integer,
    win_pct            numeric,
    record             text,
    home               text,
    road               text,
    l10                text
);

-- =========================================================
-- fct_games
-- =========================================================
create table if not exists core.fct_games (
    game_team_id       text primary key,
    game_id            text,
    team_id            integer,
    season_id          integer,
    season_num         text,
    season_team_id     text,
    game_date          date,
    matchup            text,
    ha                 text,
    wl                 text,
    min                numeric,
    pts                integer,
    fgm                integer,
    fga                integer,
    fg_pct             numeric,
    fg3m               integer,
    fg3a               integer,
    fg3_pct            numeric,
    ftm                integer,
    fta                integer,
    ft_pct             numeric,
    oreb               integer,
    dreb               integer,
    reb                integer,
    ast                integer,
    stl                integer,
    blk                integer,
    tov                integer,
    pf                 integer,
    plus_minus         numeric,
    team_abbreviation  text,
    team_name          text
);

create index if not exists idx_fct_games_game_id on core.fct_games (game_id);
create index if not exists idx_fct_games_team_id on core.fct_games (team_id);
create index if not exists idx_fct_games_season_id on core.fct_games (season_id);

-- =========================================================
-- fct_shots
-- =========================================================
create table if not exists core.fct_shots (
    game_gevent_id       text primary key,
    game_team_id         text,
    game_id              text,
    game_event_id        integer,
    season_num           text,
    game_date            date,
    team_id              integer,
    team_name            text,
    player_id            integer,
    player_name          text,
    htm                  text,
    vtm                  text,
    period               integer,
    minutes_remaining    integer,
    seconds_remaining    integer,
    grid_type            text,
    event_type           text,
    action_type          text,
    shot_type            text,
    shot_zone_basic      text,
    shot_zone_area       text,
    shot_zone_range      text,
    shot_distance        integer,
    loc_x                integer,
    loc_y                integer,
    shot_attempted_flag  smallint,
    shot_made_flag       smallint,
    zone_id              text,
    game_player_id       text
);

create index if not exists idx_fct_shots_game_id on core.fct_shots (game_id);
create index if not exists idx_fct_shots_player_id on core.fct_shots (player_id);
create index if not exists idx_fct_shots_team_id on core.fct_shots (team_id);
create index if not exists idx_fct_shots_zone_id on core.fct_shots (zone_id);

-- =========================================================
-- fct_boxscore
-- =========================================================
create table if not exists core.fct_boxscore (
    game_player_id               text primary key,
    game_id                      text,
    team_id                      integer,
    person_id                    integer,
    game_team_id                 text,
    team_city                    text,
    team_name                    text,
    team_tricode                 text,
    team_slug                    text,
    first_name                   text,
    family_name                  text,
    name_i                       text,
    player_slug                  text,
    position                     text,
    comment                      text,
    jersey_num                   text,
    minutes                      text,
    field_goals_made             integer,
    field_goals_attempted        integer,
    field_goals_percentage       numeric,
    three_pointers_made          integer,
    three_pointers_attempted     integer,
    three_pointers_percentage    numeric,
    free_throws_made             integer,
    free_throws_attempted        integer,
    free_throws_percentage       numeric,
    rebounds_offensive           integer,
    rebounds_defensive           integer,
    rebounds_total               integer,
    assists                      integer,
    steals                       integer,
    blocks                       integer,
    turnovers                    integer,
    fouls_personal               integer,
    points                       integer,
    plus_minus_points            numeric
);

create index if not exists idx_fct_boxscore_game_id on core.fct_boxscore (game_id);
create index if not exists idx_fct_boxscore_team_id on core.fct_boxscore (team_id);
create index if not exists idx_fct_boxscore_person_id on core.fct_boxscore (person_id);

-- =========================================================
-- fct_shot_zone_gp (aggregate, derived from fct_shots)
-- Materialized view so it can be refreshed automatically after
-- each shots load instead of being recreated by hand in Supabase.
-- =========================================================
create materialized view if not exists core.fct_shot_zone_gp as
select
    game_player_id,
    zone_id,
    count(*) as fga,
    sum(shot_made_flag)::int as fgm
from core.fct_shots
group by 1, 2;

-- required for REFRESH MATERIALIZED VIEW CONCURRENTLY
create unique index if not exists idx_fct_shot_zone_gp_pk
    on core.fct_shot_zone_gp (game_player_id, zone_id);
