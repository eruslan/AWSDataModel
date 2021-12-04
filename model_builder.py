import configparser
from sqlalchemy_schemadisplay import create_schema_graph
from sqlalchemy import MetaData


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    graph = create_schema_graph(metadata=MetaData('postgresql://dwhuser:Passw0rd@dwhcluster.ceaqx5n8emyi.us-west-2.redshift.amazonaws.com:5439/sparkify'))
    graph.write_png('./sparkifydb_erd.png')                       

if __name__ == "__main__":
    main()
