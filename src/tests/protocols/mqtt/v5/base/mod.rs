// NOTE: copy v3 tests as v5 base tests

mod connect;
mod pending;
mod publish;
mod retain;
mod session;
mod subscribe;
mod will;

macro_rules! assert_connack {
    ($received:expr, $expected:expr) => {
        match $received {
            Packet::Connack(mut inner) => {
                inner.properties = Default::default();
                assert_eq!(inner, $expected);
            }
            pkt => panic!("invalid connack packet: {:?}", pkt),
        }
    };
}
pub(crate) use assert_connack;
