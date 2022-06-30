use freetds_sys::*;

pub type CslibMsgCallbackType = extern "C" fn(*mut CS_CONTEXT, *const CS_CLIENTMSG) -> i32;
pub type ClientMsgCallbackType = extern "C" fn(*mut CS_CONTEXT, *mut CS_CONNECTION, *const CS_CLIENTMSG) -> i32;
pub type ServerMsgCallbackType = extern "C" fn(*mut CS_CONTEXT, *mut CS_CONNECTION, *const CS_SERVERMSG) -> i32;

pub enum Property<'a> {
    CslibMsgCallback(CslibMsgCallbackType),
    ClientMsgCallback(ClientMsgCallbackType),
    ServerMsgCallback(ServerMsgCallbackType),
    I32(i32),
    U32(u32),
    String(&'a str),
}
