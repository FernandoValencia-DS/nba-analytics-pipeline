from __future__ import annotations
import pandas as pd
from src.db.connection import get_conn
import time
from nba_api.stats.endpoints import boxscoretraditionalv3, boxscoretraditionalv2
from src.etl.transform.transform_boxscore import tr_boxscore
from requests.exceptions import ReadTimeout, ConnectionError, HTTPError, Timeout
import requests



def fetch_pending_game_ids_for_boxscore(seasons: list[str] | None = None) -> list[str]:
    conn = get_conn()
    cur = conn.cursor()

    if seasons:
        sql = """
        select distinct g.game_id
        from core.fct_games g
        left join core.fct_boxscore b
          on b.game_id = g.game_id
        where b.game_id is null
          and g.season_num = any(%s)
        order by g.game_id
        """
        cur.execute(sql, (seasons,))
    else:
        sql = """
        select distinct g.game_id
        from core.fct_games g
        left join core.fct_boxscore b
          on b.game_id = g.game_id
        where b.game_id is null
        order by g.game_id
        """
        cur.execute(sql)

    game_ids = [r[0] for r in cur.fetchall()]
    cur.close(); conn.close()
    return game_ids



def fetch_gamebox(
    game_ids: list[str],
    sleep_sec: float = 0.6,
    timeout: int = 120,
    attempts_v3: int = 3,
    attempts_v2: int = 2,
    max_consecutive_fails: int = 3,
    cooldown_sec: int = 60,
) -> pd.DataFrame:

    total = len(game_ids)
    dfs = []
    log = []
    consecutive_fails = 0

    def try_call(fn, attempts: int, base_backoff: float = 1.5):
        last_err = None
        for a in range(1, attempts + 1):
            try:
                return fn(), None
            except requests.exceptions.RequestException as e:
                last_err = e
                time.sleep(base_backoff * a)  # 1.5s, 3s, 4.5s...
            except Exception as e:
                return None, e
        return None, last_err

    for i, gid in enumerate(game_ids, start=1):
        print(f"[{i}/{total}] GAME_ID={gid} ...", end=" ")

        # --- V3 ---
        def v3():
            box = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=gid, timeout=timeout)
            return box.get_data_frames()[0]

        df, err = try_call(v3, attempts_v3)

        if df is not None and not df.empty:
            df["SOURCE_ENDPOINT"] = "traditional_v3"
            dfs.append(df)
            log.append({"game_id": gid, "status": "ok", "endpoint": "v3", "error": None})
            consecutive_fails = 0
            print("OK (V3)")
            time.sleep(sleep_sec)
            continue

        # --- V2 fallback ---
        print("V3 falló → V2...", end=" ")

        def v2():
            box = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=gid, timeout=timeout)
            return box.get_data_frames()[0]

        df2, err2 = try_call(v2, attempts_v2)

        if df2 is not None and not df2.empty:
            df2["SOURCE_ENDPOINT"] = "traditional_v2_fallback"
            dfs.append(df2)
            log.append({"game_id": gid, "status": "ok", "endpoint": "v2", "error": None})
            consecutive_fails = 0
            print("OK (V2)")
        else:
            e_use = err2 if err2 is not None else err
            consecutive_fails += 1
            msg = f"{type(e_use).__name__}: {str(e_use)[:200]}" if e_use else "Unknown error"
            log.append({"game_id": gid, "status": "fail", "endpoint": "v3_then_v2", "error": msg})
            print(f"FALLÓ ({msg})")

            # --- corta la racha de fallos con cooldown ---
            if consecutive_fails >= max_consecutive_fails:
                print(f"  → {consecutive_fails} fallos seguidos. Cooldown {cooldown_sec}s para recuperar...")
                time.sleep(cooldown_sec)
                consecutive_fails = 0

        time.sleep(sleep_sec)

    if not dfs:
        return pd.DataFrame()

    df_concat = pd.concat(dfs, ignore_index=True)
    df_transform = tr_boxscore(df_concat)

    # Opcional: devuelve log aparte si quieres (aquí lo dejo como variable que puedes inspeccionar)
    # df_log = pd.DataFrame(log)

    return df_transform
