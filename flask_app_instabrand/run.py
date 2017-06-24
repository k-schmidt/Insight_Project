"""
Insight Data Engineering
Kyle Schmidt

Main entry for the flask application
"""
from app import app


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
