// In Vector clock, everything such as send and receive counts as an event.
// In order to implement causal delivery however, we should change the way : WE ONLY TRACT MESSAGE SENDs
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use tokio::time::sleep;

#[derive(Clone, Debug)]
struct Message {
    sender: usize,
    vector_clock: Vec<i32>,
}

fn node(
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

                if let Err(err) = take_pointwise_max(vc.clone(), msg.clone()) {
                    println!(
                        "Node {} detected concurrency...
                        self : {:?}
                        other: {:?}",
                        order_in_cluster + 1,
                        vc,
                        msg
                    );
                    buffer.push(msg);
                    println!("Buffered message: {:?}", buffer);
                } else {
                    println!("Node {} received {:?}", order_in_cluster + 1, msg);
                    vc[msg.sender] += 1;

                    let mut idx = -1;
                    for (order, x) in buffer.iter().enumerate() {
                        if let Ok(found) = take_pointwise_max(vc.clone(), x.clone()) {
                            println!(
                                "Node {} found buffered vector clock {:?}\nreplaying message",
                                order_in_cluster + 1,
                                x
                            );
                            *vc = found;
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
                let delay = rand::random::<u64>() % 4;
                sleep(Duration::from_secs(delay)).await;
            }
        }
    });
}

fn take_pointwise_max(self_vc: Vec<i32>, other_vc: Message) -> Result<Vec<i32>, ()> {
    //A message sent by P1 is only delivered to P2 if the followings are met:
    //VC[P1] on P1 == VC[P1] +1 on P2
    //AND
    //T[Pk] on P1 <= VC[Pk] on P2 for all K != 1.

    if self_vc[other_vc.sender] + 1 == other_vc.vector_clock[other_vc.sender] {
        for (idx, x) in self_vc.iter().enumerate() {
            if idx != other_vc.sender && x > &other_vc.vector_clock[idx] {
                return Err(());
            }
        }
        Ok(other_vc.vector_clock)
    } else {
        Err(())
    }
}

#[tokio::test]
pub async fn vector_clock_test() {
    let (tx1, rx1) = tokio::sync::mpsc::channel(10);
    let (tx2, rx2) = tokio::sync::mpsc::channel(10);
    let (tx3, rx3) = tokio::sync::mpsc::channel(10);

    node(0, vec![tx2.clone(), tx3.clone()], rx1);
    node(1, vec![tx1.clone(), tx3.clone()], rx2);
    node(2, vec![tx1.clone(), tx2.clone()], rx3);

    sleep(Duration::from_secs(10)).await;
}
