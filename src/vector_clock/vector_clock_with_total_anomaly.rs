// ! This module is to prove that vector clock cannot solve total delivery anomalies
use crate::vector_clock::take_pointwise_max;
use crate::vector_clock::Message;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use tokio::time::sleep;

fn total_anomaly_generating_node(
    order_in_cluster: usize,
    outbound_channels: Vec<Sender<Message>>,
    mut recv: Receiver<Message>,
) {
    let vc = Arc::new(RwLock::new(vec![0, 0, 0]));
    tokio::spawn({
        let vc_for_recv = vc.clone();
        async move {
            let mut buffer: Vec<Message> = Vec::new();
            while let Some(msg) = recv.recv().await {
                let mut vc = vc_for_recv.write().await;

                if let Err(()) = take_pointwise_max(vc.clone(), msg.clone()) {
                    println!(
                        "Node {} detected concurrency...
                        self : {:?}
                        other: {:?}",
                        order_in_cluster, vc, msg
                    );
                    buffer.push(msg);
                    println!("Buffered message: {:?}", buffer);
                } else {
                    println!("Node {} received {:?}", order_in_cluster, msg);
                    vc[msg.sender] += 1;

                    let mut idx = -1;
                    for (order, x) in buffer.iter().enumerate() {
                        if let Ok(()) = take_pointwise_max(vc.clone(), x.clone()) {
                            println!(
                                "Node {} found buffered vector clock {:?}\nreplaying message",
                                order_in_cluster, x
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
        }
    });
    tokio::spawn({
        let vc_for_send = vc.clone();
        async move {
            loop {
                {
                    let mut vc = vc_for_send.write().await;
                    vc[order_in_cluster] += 1;
                }
                for channel in outbound_channels.iter() {
                    let vc = vc_for_send.read().await.clone();
                    if channel
                        .send(Message {
                            sender: order_in_cluster,
                            vector_clock: vc,
                        })
                        .await
                        .is_err()
                    {
                        eprintln!("Failed to send vector clock");
                    }
                }
                let delay = rand::random::<u64>() % 3;
                sleep(Duration::from_secs(delay)).await;
            }
        }
    });
}

// #[tokio::test]
// pub async fn vector_clock_test() {
//     let (tx1, rx1) = tokio::sync::mpsc::channel(10);
//     let (tx2, rx2) = tokio::sync::mpsc::channel(10);
//     let (tx3, rx3) = tokio::sync::mpsc::channel(10);

//     total_anomaly_generating_node(0, vec![tx2.clone(), tx3.clone()], rx1);
//     total_anomaly_generating_node(1, vec![tx1.clone(), tx3.clone()], rx2);
//     total_anomaly_generating_node(2, vec![tx1.clone(), tx2.clone()], rx3);

//     sleep(Duration::from_secs(10)).await;
// }
