use tokio::sync::mpsc::{Receiver, Sender};

use super::{take_pointwise_max, Command, Message};

async fn node(
    order_in_cluster: usize,
    outbound_channels: Vec<Sender<Command>>,
    mut recv: Receiver<Command>,
) {
    let mut vc = vec![0, 0, 0];

    let mut buffer: Vec<Message> = Vec::new();
    while let Some(msg) = recv.recv().await {
        match msg {
            Command::PeerMessage(msg) => {
                if let Err(()) = take_pointwise_max(vc.clone(), msg.clone()) {
                    println!(
                        "\n---------Node {order_in_cluster} detected concurrency...-----------\n
self : {vc:?}
other: {msg:?}\n
---------------------------------------------------
",
                    );
                    buffer.push(msg);
                } else {
                    println!("Node {} received {:?}", order_in_cluster, msg);
                    vc[msg.sender] += 1;

                    let mut idx = -1;
                    for (order, x) in buffer.iter().enumerate() {
                        if let Ok(()) = take_pointwise_max(vc.clone(), x.clone()) {
                            println!(
                                "Node {order_in_cluster} found buffered vector clock {x:?} replaying message...",
                            );
                            vc[msg.sender] += 1;
                            idx = order as i32;
                        }
                    }
                    if idx != -1 {
                        buffer.remove(idx as usize);
                    }
                }
            }
            Command::ClientRequest(delay) => {
                //event happens
                vc[order_in_cluster] += 1;
                fire(
                    vc.clone(),
                    order_in_cluster,
                    outbound_channels.clone(),
                    delay,
                )
                .await;
            }
        }
    }
}

async fn fire(
    vc: Vec<i32>,
    order_in_cluster: usize,
    outbound_channels: Vec<Sender<Command>>,
    delay: bool,
) {
    let msg = Command::PeerMessage(Message {
        sender: order_in_cluster,
        vector_clock: vc.clone(),
    });

    outbound_channels[0].send(msg.clone()).await.unwrap();

    tokio::spawn(async move {
        if delay {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            outbound_channels[1].send(msg).await.unwrap();
        } else {
            outbound_channels[1].send(msg).await.unwrap();
        }
    });
}

#[tokio::test]
pub async fn vector_clock_test_buffer() {
    let (tx1, rx1) = tokio::sync::mpsc::channel(10);
    let (tx2, rx2) = tokio::sync::mpsc::channel(10);
    let (tx3, rx3) = tokio::sync::mpsc::channel(10);

    tokio::spawn(node(0, vec![tx2.clone(), tx3.clone()], rx1));
    tokio::spawn(node(1, vec![tx1.clone(), tx3.clone()], rx2));
    tokio::spawn(node(2, vec![tx1.clone(), tx2.clone()], rx3));

    tx1.send(Command::ClientRequest(true)).await.unwrap();

    // Without delay, sending is too fast and
    tokio::time::sleep(std::time::Duration::from_micros(1)).await;
    tx2.send(Command::ClientRequest(false)).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
}
