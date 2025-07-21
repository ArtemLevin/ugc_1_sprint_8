from app.factory import create_app
app = create_app()

if __name__ == "__main__":
    app = create_app()
    import eventlet
    eventlet.wsgi.server(eventlet.listen(('', 5000)), app)