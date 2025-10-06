# app.py
from flask import Flask, jsonify, abort, send_file, request
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import OperationalError

_engine = None

def get_engine():
    global _engine
    if _engine is not None:
        return _engine
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise RuntimeError("Missing DB_URL (or DATABASE_URL) environment variable.")
    if db_url.startswith("postgres://"):
        db_url = "postgresql://" + db_url[len("postgres://"):]
    _engine = create_engine(
        db_url,
        pool_pre_ping=True,
    )
    return _engine

def create_app():
    app = Flask(__name__)

    @app.get("/", endpoint="health")
    def health():
        return "<p>Server working!</p>"

    @app.get("/img", endpoint="show_img")
    def show_img():
        return send_file("amygdala.gif", mimetype="image/gif")

    @app.get("/dissociate/terms/<term_a>/<term_b>", endpoint="dissociate_terms")
    def dissociate_by_terms(term_a, term_b):
        """Returns studies that mention term_a but NOT term_b"""
        try:
            eng = get_engine()
            with eng.begin() as conn:
                conn.execute(text("SET search_path TO ns, public;"))
                
                query = text("""
                    SELECT DISTINCT a1.study_id
                    FROM ns.annotations_terms a1
                    WHERE a1.term = :term_a
                    AND a1.study_id NOT IN (
                        SELECT study_id 
                        FROM ns.annotations_terms 
                        WHERE term = :term_b
                    )
                    ORDER BY a1.study_id
                    LIMIT 100
                """)
                
                result = conn.execute(query, {
                    "term_a": term_a,
                    "term_b": term_b
                })
                
                studies = [row[0] for row in result]
                
                return jsonify({
                    "term_a": term_a,
                    "term_b": term_b,
                    "count": len(studies),
                    "studies": studies
                }), 200
                
        except Exception as e:
            return jsonify({
                "error": str(e),
                "term_a": term_a,
                "term_b": term_b
            }), 500

    @app.get("/dissociate/locations/<coords_a>/<coords_b>", endpoint="dissociate_locations")
    def dissociate_by_coordinates(coords_a, coords_b):
        """
        Returns studies near coords_a but NOT near coords_b
        Optional query parameter: r=radius (default 20mm)
        Example: /dissociate/locations/0_-52_26/-2_50_-6?r=15
        """
        try:
            x1, y1, z1 = map(float, coords_a.split("_"))
            x2, y2, z2 = map(float, coords_b.split("_"))
            
            # Get radius from query parameter, default to 20mm
            radius = float(request.args.get('r', 20.0))
            
            eng = get_engine()
            with eng.begin() as conn:
                conn.execute(text("SET search_path TO ns, public;"))
                
                # Query uses 3D Euclidean distance
                query = text("""
                    SELECT DISTINCT c1.study_id,
                           ST_X(c1.geom) as x,
                           ST_Y(c1.geom) as y,
                           ST_Z(c1.geom) as z,
                           SQRT(
                               POWER(ST_X(c1.geom) - :x1, 2) + 
                               POWER(ST_Y(c1.geom) - :y1, 2) + 
                               POWER(ST_Z(c1.geom) - :z1, 2)
                           ) as distance_a
                    FROM ns.coordinates c1
                    WHERE SQRT(
                        POWER(ST_X(c1.geom) - :x1, 2) + 
                        POWER(ST_Y(c1.geom) - :y1, 2) + 
                        POWER(ST_Z(c1.geom) - :z1, 2)
                    ) <= :radius
                    AND c1.study_id NOT IN (
                        SELECT study_id
                        FROM ns.coordinates
                        WHERE SQRT(
                            POWER(ST_X(geom) - :x2, 2) + 
                            POWER(ST_Y(geom) - :y2, 2) + 
                            POWER(ST_Z(geom) - :z2, 2)
                        ) <= :radius
                    )
                    ORDER BY distance_a
                    LIMIT 100
                """)
                
                result = conn.execute(query, {
                    "x1": x1, "y1": y1, "z1": z1,
                    "x2": x2, "y2": y2, "z2": z2,
                    "radius": radius
                })
                
                studies = []
                for row in result:
                    studies.append({
                        "study_id": row[0],
                        "x": float(row[1]),
                        "y": float(row[2]),
                        "z": float(row[3]),
                        "distance_from_a": float(row[4])
                    })
                
                return jsonify({
                    "coords_a": [x1, y1, z1],
                    "coords_b": [x2, y2, z2],
                    "radius_mm": radius,
                    "count": len(studies),
                    "studies": studies
                }), 200
                
        except ValueError as ve:
            return jsonify({
                "error": "Invalid format. Use x_y_z with numbers",
                "coords_a": coords_a,
                "coords_b": coords_b
            }), 400
        except Exception as e:
            return jsonify({
                "error": str(e),
                "coords_a": coords_a,
                "coords_b": coords_b
            }), 500

    @app.get("/test_db", endpoint="test_db")
    def test_db():
        eng = get_engine()
        payload = {"ok": False, "dialect": eng.dialect.name}

        try:
            with eng.begin() as conn:
                conn.execute(text("SET search_path TO ns, public;"))
                payload["version"] = conn.exec_driver_sql("SELECT version()").scalar()

                payload["coordinates_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.coordinates")).scalar()
                payload["metadata_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.metadata")).scalar()
                payload["annotations_terms_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.annotations_terms")).scalar()

                try:
                    rows = conn.execute(text(
                        "SELECT study_id, ST_X(geom) AS x, ST_Y(geom) AS y, ST_Z(geom) AS z FROM ns.coordinates LIMIT 3"
                    )).mappings().all()
                    payload["coordinates_sample"] = [dict(r) for r in rows]
                except Exception:
                    payload["coordinates_sample"] = []

                try:
                    rows = conn.execute(text("SELECT * FROM ns.metadata LIMIT 3")).mappings().all()
                    payload["metadata_sample"] = [dict(r) for r in rows]
                except Exception:
                    payload["metadata_sample"] = []

                try:
                    rows = conn.execute(text(
                        "SELECT study_id, contrast_id, term, weight FROM ns.annotations_terms LIMIT 3"
                    )).mappings().all()
                    payload["annotations_terms_sample"] = [dict(r) for r in rows]
                except Exception:
                    payload["annotations_terms_sample"] = []

            payload["ok"] = True
            return jsonify(payload), 200

        except Exception as e:
            payload["error"] = str(e)
            return jsonify(payload), 500

    return app

app = create_app()