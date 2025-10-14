from flask import Flask, jsonify, abort, send_file
import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

_engine = None

def get_engine():
    global _engine
    if _engine is not None:
        return _engine
    
    # Fix: Get the environment variable, not the URL string itself
    db_url = os.getenv("DB_URL") or os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("Missing DB_URL (or DATABASE_URL) environment variable.")
    
    # Normalize old 'postgres://' scheme to 'postgresql://'
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

    @app.get("/terms/<term>/studies", endpoint="terms_studies")
    def get_studies_by_term(term):
        """Get studies that mention a specific term"""
        eng = get_engine()
        try:
            with eng.begin() as conn:
                conn.execute(text("SET search_path TO ns, public;"))
                studies = conn.execute(text("""
                    SELECT DISTINCT study_id
                    FROM ns.annotations_terms
                    WHERE term = :term
                """), {"term": term}).scalars().all()
                
                return jsonify({
                    "term": term,
                    "studies": list(studies),
                    "count": len(studies)
                })
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.get("/locations/<coords>/studies", endpoint="locations_studies")
    def get_studies_by_coordinates(coords):
        """Get studies that report a specific MNI coordinate"""
        try:
            x, y, z = map(int, coords.split("_"))
        except Exception:
            abort(400, "Coordinates must be in x_y_z format.")
        
        eng = get_engine()
        try:
            with eng.begin() as conn:
                conn.execute(text("SET search_path TO ns, public;"))
                studies = conn.execute(text("""
                    SELECT DISTINCT study_id
                    FROM ns.coordinates
                    WHERE ST_X(geom) = :x AND ST_Y(geom) = :y AND ST_Z(geom) = :z
                """), {"x": x, "y": y, "z": z}).scalars().all()
                
                return jsonify({
                    "coordinates": [x, y, z],
                    "studies": list(studies),
                    "count": len(studies)
                })
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.get("/test_db", endpoint="test_db")
    def test_db():
        """Test database connectivity and show sample data"""
        eng = get_engine()
        payload = {"ok": False, "dialect": eng.dialect.name}

        try:
            with eng.begin() as conn:
                # Ensure we are in the correct schema
                conn.execute(text("SET search_path TO ns, public;"))
                payload["version"] = conn.exec_driver_sql("SELECT version()").scalar()

                # Counts
                payload["coordinates_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.coordinates")).scalar()
                payload["metadata_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.metadata")).scalar()
                payload["annotations_terms_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.annotations_terms")).scalar()

                # Samples
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

    @app.get("/dissociate/terms/<term_a>/<term_b>", endpoint="dissociate_terms")
    def dissociate_terms(term_a, term_b):
        """Find studies that mention one term but not the other"""
        eng = get_engine()
        try:
            with eng.begin() as conn:
                conn.execute(text("SET search_path TO ns, public;"))
                
                # Studies mentioning term_a but not term_b
                studies_a = conn.execute(text("""
                    SELECT DISTINCT study_id
                    FROM ns.annotations_terms
                    WHERE term = :term_a
                    AND study_id NOT IN (
                        SELECT study_id FROM ns.annotations_terms WHERE term = :term_b
                    )
                """), {"term_a": term_a, "term_b": term_b}).scalars().all()

                # Studies mentioning term_b but not term_a
                studies_b = conn.execute(text("""
                    SELECT DISTINCT study_id
                    FROM ns.annotations_terms
                    WHERE term = :term_b
                    AND study_id NOT IN (
                        SELECT study_id FROM ns.annotations_terms WHERE term = :term_a
                    )
                """), {"term_a": term_a, "term_b": term_b}).scalars().all()

                payload = {
                    "A_minus_B": list(studies_a),
                    "B_minus_A": list(studies_b),
                    "term_a": term_a,
                    "term_b": term_b,
                    "count_a_minus_b": len(studies_a),
                    "count_b_minus_a": len(studies_b)
                }
                return jsonify(payload)
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.get("/dissociate/locations/<coords_a>/<coords_b>", endpoint="dissociate_locations")
    def dissociate_locations(coords_a, coords_b):
        """Find studies that report one coordinate but not the other"""
        eng = get_engine()
        try:
            x1, y1, z1 = map(int, coords_a.split("_"))
            x2, y2, z2 = map(int, coords_b.split("_"))
        except Exception:
            abort(400, "Coordinates must be in x_y_z format.")

        try:
            with eng.begin() as conn:
                conn.execute(text("SET search_path TO ns, public;"))
                
                # Studies mentioning coords_a but not coords_b
                studies_a = conn.execute(text("""
                    SELECT DISTINCT study_id
                    FROM ns.coordinates
                    WHERE ST_X(geom) = :x1 AND ST_Y(geom) = :y1 AND ST_Z(geom) = :z1
                    AND study_id NOT IN (
                        SELECT study_id FROM ns.coordinates
                        WHERE ST_X(geom) = :x2 AND ST_Y(geom) = :y2 AND ST_Z(geom) = :z2
                    )
                """), {"x1": x1, "y1": y1, "z1": z1, "x2": x2, "y2": y2, "z2": z2}).scalars().all()

                # Studies mentioning coords_b but not coords_a
                studies_b = conn.execute(text("""
                    SELECT DISTINCT study_id
                    FROM ns.coordinates
                    WHERE ST_X(geom) = :x2 AND ST_Y(geom) = :y2 AND ST_Z(geom) = :z2
                    AND study_id NOT IN (
                        SELECT study_id FROM ns.coordinates
                        WHERE ST_X(geom) = :x1 AND ST_Y(geom) = :y1 AND ST_Z(geom) = :z1
                    )
                """), {"x1": x1, "y1": y1, "z1": z1, "x2": x2, "y2": y2, "z2": z2}).scalars().all()

                payload = {
                    "A_minus_B": list(studies_a),
                    "B_minus_A": list(studies_b),
                    "coords_a": [x1, y1, z1],
                    "coords_b": [x2, y2, z2],
                    "count_a_minus_b": len(studies_a),
                    "count_b_minus_a": len(studies_b)
                }
                return jsonify(payload)
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    return app

# WSGI entry point
app = create_app()