pub mod node;
mod vector_clock_with_total_anomaly;

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
pub fn take_pointwise_max(self_vc: Vec<i32>, other_vc: Message) -> Result<(), ()> {
    //A message sent by P1 is only delivered to P2 if the followings are met:
    //VC[P1] on P1 == VC[P1] +1 on P2
    //AND
    //T[Pk] on P1 <= VC[Pk] on P2 for all K != 1.
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
