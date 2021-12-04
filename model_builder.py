import configparser
from sqlalchemy_schemadisplay import create_schema_graph
from sqlalchemy import MetaData


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    graph = create_schema_graph(metadata=MetaData(
        'postgresql://{}:{}@{}/{}'.format(config.get("CLUSTER", "DB_USER"),
                                                        config.get("CLUSTER", "DB_PASSWORD"), \
                                                        config.get("CLUSTER", "HOST"), \
                                                        config.get("CLUSTER", "DB_NAME")
                                                    )
                                                    )
                                )
    graph.write_png('./sparkifydb_erd.png')
if __name__ == "__main__":
    main()
