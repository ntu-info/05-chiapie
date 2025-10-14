import os
from flask import Flask, jsonify, request
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

app = Flask(__name__)

# Database configuration
def get_engine():
    """Create SQLAlchemy engine from environment variable"""
    db_url = os.environ.get("DB_URL")  # FIX: use environment variable
    if not db_url:
        raise ValueError("DB_URL environment variable not set")
    return create_engine(db_url, poolclass=NullPool)

@app.route('/')
def home():
    """API documentation endpoint"""
    return jsonify({
        "service": "Neurosynth Dissociation API",
        "version": "1.0",
        "endpoints": {
            "dissociate_terms": {
                "path": "/dissociate/terms/<term_a>/<term_b>",
                "method": "GET",
                "description": "Returns studies mentioning term_a but NOT term_b",
                "parameters": {
                    "format": "Optional query param: 'html' for HTML table view"
                },
                "example": "/dissociate/terms/posterior_cingulate/ventromedial_prefrontal"
            },
            "dissociate_locations": {
                "path": "/dissociate/locations/<coords_a>/<coords_b>",
                "method": "GET",
                "description": "Returns studies near coords_a but NOT near coords_b",
                "parameters": {
                    "format": "Optional query param: 'html' for HTML table view",
                    "threshold": "Optional query param: distance threshold in mm (default: 10)"
                },
                "example": "/dissociate/locations/0_-52_26/-2_50_-6"
            }
        }
    }), 200

@app.route('/dissociate/terms/<term_a>/<term_b>')
def dissociate_by_terms(term_a, term_b):
    """Returns studies that mention term_a but NOT term_b"""
    try:
        eng = get_engine()
        with eng.begin() as conn:
            conn.execute(text("SET search_path TO ns, public;"))
            
            query = text("""
                SELECT DISTINCT a1.study_id, m.title
                FROM ns.annotations_terms a1
                LEFT JOIN ns.metadata m ON a1.study_id = m.study_id
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
            
            studies = []
            for row in result:
                studies.append({
                    "study_id": row[0],
                    "title": row[1] if row[1] else "N/A"
                })
            
            # Check if HTML format is requested
            if request.args.get('format') == 'html':
                html = f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Dissociation Results: {term_a} \ {term_b}</title>
                    <style>
                        body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
                        .container {{ max-width: 1200px; margin: 0 auto; background-color: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
                        h2 {{ color: #333; border-bottom: 3px solid #4CAF50; padding-bottom: 10px; }}
                        table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
                        th {{ background-color: #4CAF50; color: white; padding: 12px; text-align: left; position: sticky; top: 0; }}
                        td {{ border: 1px solid #ddd; padding: 8px; }}
                        tr:nth-child(even) {{ background-color: #f2f2f2; }}
                        tr:hover {{ background-color: #e0e0e0; }}
                        .info {{ margin-bottom: 20px; padding: 15px; background-color: #e7f3fe; border-left: 6px solid #2196F3; border-radius: 4px; }}
                        .info strong {{ color: #1976D2; }}
                        .title-cell {{ max-width: 600px; word-wrap: break-word; }}
                    </style>
                </head>
                <body>
                    <div class="container">
                        <h2>Term Dissociation Results</h2>
                        <div class="info">
                            <strong>Studies with:</strong> {term_a}<br>
                            <strong>But NOT with:</strong> {term_b}<br>
                            <strong>Total Studies Found:</strong> {len(studies)}<br>
                            <strong>Limit:</strong> 100 studies
                        </div>
                        <table>
                            <tr>
                                <th style="width: 50px;">#</th>
                                <th style="width: 150px;">Study ID</th>
                                <th>Title</th>
                            </tr>
                """
                for i, study in enumerate(studies, 1):
                    title = study['title'].replace('<', '&lt;').replace('>', '&gt;')
                    html += f"""
                            <tr>
                                <td>{i}</td>
                                <td>{study['study_id']}</td>
                                <td class="title-cell">{title}</td>
                            </tr>
                    """
                html += """
                        </table>
                    </div>
                </body>
                </html>
                """
                return html
            
            # Default JSON response
            return jsonify({
                "term_a": term_a,
                "term_b": term_b,
                "description": f"Studies mentioning '{term_a}' but NOT '{term_b}'",
                "count": len(studies),
                "limit": 100,
                "studies": studies
            }), 200
            
    except Exception as e:
        return jsonify({
            "error": str(e),
            "term_a": term_a,
            "term_b": term_b
        }), 500

@app.route('/dissociate/locations/both/<coords_a>/<coords_b>')
def dissociate_locations_both(coords_a, coords_b):
    """Returns studies for both directions: A\B and B\A"""
    def get_studies(x1, y1, z1, x2, y2, z2, threshold):
        eng = get_engine()
        with eng.begin() as conn:
            conn.execute(text("SET search_path TO ns, public;"))
            query = text("""
                SELECT DISTINCT c1.study_id,
                       ST_X(c1.geom) as x,
                       ST_Y(c1.geom) as y,
                       ST_Z(c1.geom) as z,
                       ST_Distance(
                           c1.geom,
                           ST_SetSRID(ST_MakePoint(:x1, :y1, :z1), 4326)
                       ) as distance_a
                FROM ns.coordinates c1
                WHERE ST_Distance(
                    c1.geom,
                    ST_SetSRID(ST_MakePoint(:x1, :y1, :z1), 4326)
                ) <= :threshold
                AND c1.study_id NOT IN (
                    SELECT study_id
                    FROM ns.coordinates
                    WHERE ST_Distance(
                        geom,
                        ST_SetSRID(ST_MakePoint(:x2, :y2, :z2), 4326)
                    ) <= :threshold
                )
                ORDER BY distance_a
                LIMIT 100
            """)
            result = conn.execute(query, {
                "x1": x1, "y1": y1, "z1": z1,
                "x2": x2, "y2": y2, "z2": z2,
                "threshold": threshold
            })
            studies = []
            for row in result:
                studies.append({
                    "study_id": row[0],
                    "coordinates": {
                        "x": round(float(row[1]), 2),
                        "y": round(float(row[2]), 2),
                        "z": round(float(row[3]), 2)
                    },
                    "distance_from_a_mm": round(float(row[4]), 2)
                })
            return studies

    try:
        parts_a = coords_a.split("_")
        parts_b = coords_b.split("_")
        if len(parts_a) != 3 or len(parts_b) != 3:
            raise ValueError("Coordinates must be in x_y_z format")
        x1, y1, z1 = float(parts_a[0]), float(parts_a[1]), float(parts_a[2])
        x2, y2, z2 = float(parts_b[0]), float(parts_b[1]), float(parts_b[2])
        threshold = float(request.args.get('threshold', 10.0))

        studies_a_b = get_studies(x1, y1, z1, x2, y2, z2, threshold)
        studies_b_a = get_studies(x2, y2, z2, x1, y1, z1, threshold)

        return jsonify({
            "A_minus_B": {
                "coords_a": {"x": x1, "y": y1, "z": z1},
                "coords_b": {"x": x2, "y": y2, "z": z2},
                "count": len(studies_a_b),
                "studies": studies_a_b
            },
            "B_minus_A": {
                "coords_a": {"x": x2, "y": y2, "z": z2},
                "coords_b": {"x": x1, "y": y1, "z": z1},
                "count": len(studies_b_a),
                "studies": studies_b_a
            },
            "threshold_mm": threshold
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return jsonify({
        "error": "Endpoint not found",
        "message": "Visit / for API documentation"
    }), 404

@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors"""
    return jsonify({
        "error": "Internal server error",
        "message": str(error)
    }), 500

if __name__ == '__main__':
    # Development server only
    app.run(debug=True, host='0.0.0.0', port=5000)