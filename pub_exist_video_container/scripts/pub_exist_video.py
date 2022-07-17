#!/usr/bin/env python

import rabbitpy
import logging
import click

def get_tx_and_chan(schema, host, port, login, password, vhost):
    
    base_url = f"{schema}://{login}:{password}@{host}:{port}/{vhost}"
    conn = rabbitpy.Connection(base_url)
    chan = conn.channel()
    tx = rabbitpy.Tx(chan)

    return tx, chan

def pub_video_id_list(tx, chan, routing_key, video_id_list):
    tx.select()

    for video_id in video_id_list:
        msg = rabbitpy.Message(chan, video_id)
        msg.publish("", routing_key)

    tx.commit()
        
logging.basicConfig(level=logging.INFO)

@click.command()
@click.option(
    "--schema",
    type=str,
    required=True,
    help="RabbitMQ schema"
)
@click.option( 
    "--host",
    type=str,
    required=True,
    help="RabbitMQ host"
)
@click.option(
    "--port",
    type=str,
    required=True,
    help="RabbitMQ port"
)
@click.option(
    "--login",
    type=str,
    required=True,
    help="RabbitMQ login"
)    
@click.option( 
    "--password",
    type=str,
    required=True,
    help="RabbitMQ password"
)  
@click.option(
    "--vhost",
    type=str,
    required=True,
    help="RabbitMQ vhost"
)  
@click.option(
    "--routing_key",
    type=str,
    required=True,
    help="RabbitMQ queue routing_key"
)
@click.option(
    "--exist_video_id_list",
    type=str,
    required=True,
    help="exist videoid list"
)             
def main(schema, host, port, login, password, vhost, routing_key, exist_video_id_list):
    tx, chan = get_tx_and_chan(schema, host, port, login, password, vhost)
    if isinstance(exist_video_id_list, str):
        exist_video_id_list = eval(exist_video_id_list)
    pub_video_id_list(tx, chan, routing_key, exist_video_id_list[:3]) #for test
    
if __name__=="__main__":
    main()