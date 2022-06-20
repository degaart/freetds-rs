use freetds_sys::*;

pub fn type_name(type_: i32) -> Option<&'static str> {
    match type_ {
        CS_CLIENTMSG_TYPE => Some("CS_CLIENTMSG_TYPE"),
        CS_SERVERMSG_TYPE => Some("CS_SERVERMSG_TYPE"),
        CS_ALLMSG_TYPE => Some("CS_ALLMSG_TYPE"),
        CS_ILLEGAL_TYPE => Some("CS_ILLEGAL_TYPE"),
        CS_CHAR_TYPE => Some("CS_CHAR_TYPE"),
        CS_BINARY_TYPE => Some("CS_BINARY_TYPE"),
        CS_LONGCHAR_TYPE => Some("CS_LONGCHAR_TYPE"),
        CS_LONGBINARY_TYPE => Some("CS_LONGBINARY_TYPE"),
        CS_TEXT_TYPE => Some("CS_TEXT_TYPE"),
        CS_IMAGE_TYPE => Some("CS_IMAGE_TYPE"),
        CS_TINYINT_TYPE => Some("CS_TINYINT_TYPE"),
        CS_SMALLINT_TYPE => Some("CS_SMALLINT_TYPE"),
        CS_INT_TYPE => Some("CS_INT_TYPE"),
        CS_REAL_TYPE => Some("CS_REAL_TYPE"),
        CS_FLOAT_TYPE => Some("CS_FLOAT_TYPE"),
        CS_BIT_TYPE => Some("CS_BIT_TYPE"),
        CS_DATETIME_TYPE => Some("CS_DATETIME_TYPE"),
        CS_DATETIME4_TYPE => Some("CS_DATETIME4_TYPE"),
        CS_MONEY_TYPE => Some("CS_MONEY_TYPE"),
        CS_MONEY4_TYPE => Some("CS_MONEY4_TYPE"),
        CS_NUMERIC_TYPE => Some("CS_NUMERIC_TYPE"),
        CS_DECIMAL_TYPE => Some("CS_DECIMAL_TYPE"),
        CS_VARCHAR_TYPE => Some("CS_VARCHAR_TYPE"),
        CS_VARBINARY_TYPE => Some("CS_VARBINARY_TYPE"),
        CS_LONG_TYPE => Some("CS_LONG_TYPE"),
        CS_SENSITIVITY_TYPE => Some("CS_SENSITIVITY_TYPE"),
        CS_BOUNDARY_TYPE => Some("CS_BOUNDARY_TYPE"),
        CS_VOID_TYPE => Some("CS_VOID_TYPE"),
        CS_USHORT_TYPE => Some("CS_USHORT_TYPE"),
        CS_UNICHAR_TYPE => Some("CS_UNICHAR_TYPE"),
        CS_BLOB_TYPE => Some("CS_BLOB_TYPE"),
        CS_DATE_TYPE => Some("CS_DATE_TYPE"),
        CS_TIME_TYPE => Some("CS_TIME_TYPE"),
        CS_UNITEXT_TYPE => Some("CS_UNITEXT_TYPE"),
        CS_BIGINT_TYPE => Some("CS_BIGINT_TYPE"),
        CS_USMALLINT_TYPE => Some("CS_USMALLINT_TYPE"),
        CS_UINT_TYPE => Some("CS_UINT_TYPE"),
        CS_UBIGINT_TYPE => Some("CS_UBIGINT_TYPE"),
        CS_XML_TYPE => Some("CS_XML_TYPE"),
        CS_BIGDATETIME_TYPE => Some("CS_BIGDATETIME_TYPE"),
        CS_BIGTIME_TYPE => Some("CS_BIGTIME_TYPE"),
        CS_UNIQUE_TYPE => Some("CS_UNIQUE_TYPE"),
        _ => None        
    }
}

pub fn format_name(format: i32) -> Option<&'static str> {
    match format as u32{
        CS_FMT_UNUSED => Some("CS_FMT_UNUSED"),
        CS_FMT_NULLTERM => Some("CS_FMT_NULLTERM"),
        CS_FMT_PADNULL => Some("CS_FMT_PADNULL"),
        CS_FMT_PADBLANK => Some("CS_FMT_PADBLANK"),
        CS_FMT_JUSTIFY_RT => Some("CS_FMT_JUSTIFY_RT"),
        _ => None
    }
}

pub fn return_name(ret: i32) -> Option<&'static str> {
    match ret {
        CS_FAIL => Some("CS_FAIL"),
        CS_MEM_ERROR => Some("CS_MEM_ERROR"),
        CS_PENDING => Some("CS_PENDING"),
        CS_QUIET => Some("CS_QUIET"),
        CS_BUSY => Some("CS_BUSY"),
        CS_INTERRUPT => Some("CS_INTERRUPT"),
        CS_BLK_HAS_TEXT => Some("CS_BLK_HAS_TEXT"),
        CS_CONTINUE => Some("CS_CONTINUE"),
        CS_FATAL => Some("CS_FATAL"),
        CS_RET_HAFAILOVER => Some("CS_RET_HAFAILOVER"),
        CS_UNSUPPORTED => Some("CS_UNSUPPORTED"),
        CS_CANCELED => Some("CS_CANCELED"),
        CS_ROW_FAIL => Some("CS_ROW_FAIL"),
        CS_END_DATA => Some("CS_END_DATA"),
        CS_END_RESULTS => Some("CS_END_RESULTS"),
        CS_END_ITEM => Some("CS_END_ITEM"),
        CS_NOMSG => Some("CS_NOMSG"),
        CS_TIMED_OUT => Some("CS_TIMED_OUT"),
        _ => None
    }
}