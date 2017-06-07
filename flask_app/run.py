"""
Insight Data Engineering
Kyle Schmidt

Startup Script
"""
from pkg import create_app, db, app as application

if __name__ == '__main__':
    application = create_app(application)
    with application.app_context():
        db.create_all()
    application.run()
