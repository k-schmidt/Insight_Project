"""
Insight Data Engineering
Kyle Schmidt

Startup Script
"""
from pkg import app, db

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run()
