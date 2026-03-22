def connect_to_database(host, port, database, user, password):
    import psycopg2
    print("Connecting to PostgreSQL database...")
    try:
        connection = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        print("Connection to PostgreSQL database successful")
        return connection
    except Exception as e:
        print(f"Error connecting to PostgreSQL database: {e}")
        return None
