/// Vector clock is a way to keep track of the order of events in a distributed system.
/// This solves causal anomalies
mod vector_clock_with_total_anomaly;
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Clone, Debug)]
pub struct Message {
    pub sender: usize,
    pub vector_clock: Vec<i32>,
}

#[derive(Clone, Debug)]
pub enum Command {
    PeerMessage(Message),
    ClientRequest(bool),
}

//A message sent by P1 is only delivered to P2 if the followings are met:
//VC[P1] on P1 == VC[P1] +1 on P2
//AND
//T[Pk] on P1 <= VC[Pk] on P2 for all K != 1.
pub fn take_pointwise_max(self_vc: Vec<i32>, other_vc: Message) -> Result<(), ()> {
    if !((self_vc[other_vc.sender] + 1) == other_vc.vector_clock[other_vc.sender]) {
        return Err(());
    }

    for (_, (self_v, other_v)) in self_vc
        .iter()
        .zip(other_vc.vector_clock.iter())
        .enumerate()
        .filter(|(idx, _)| *idx != other_vc.sender)
    {
        if other_v > self_v {
            return Err(());
        }
    }
    Ok(())
}

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
                        "\n\n\n---------Node {order_in_cluster} detected CONCURRENCY...-----------\n
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
"\n\n\n---------Node {order_in_cluster} found befurred vector clock-----------\n
current vector clock :{vc:?}\n
replaying message :{x:?} ... \n
---------------------------------------------------\n
",
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
