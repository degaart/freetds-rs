use std::mem;
use std::fmt::Debug;
use freetds_sys::*;
use crate::command::CommandArg;
use crate::error::err;
use crate::{command::Command, connection::Connection, error::Error};
use crate::Result;

#[derive(Debug, Clone, Default)]
struct Bind {
    fmt: CS_DATAFMT,
    buffer: Vec<u8>,
    data_length: i32,
    indicator: i16
}

#[derive(PartialEq)]
enum StatementState {
    New,
    ResultsReady,
    ResultSetDone,
    Done,
}

pub struct Statement {
    command: Command,
    state: StatementState,
    binds: Vec<Bind>,
    has_errors: bool,
}

impl Statement {
    pub fn new(conn: &mut Connection) -> Self {
        let command = Command::new(conn);
        Self {
            command: command,
            state: StatementState::New,
            binds: Vec::new(),
            has_errors: false
        }
    }

    pub fn execute(&mut self, text: impl AsRef<str>) -> Result<bool> {
        if self.state != StatementState::New {
            return err!("Invalid statement state");
        }

        self.command.command(CS_LANG_CMD, CommandArg::String(text.as_ref()), CS_UNUSED)?;
        self.command.send()?;
        self.get_row_result()?;
        if self.has_errors {
            err!("An error occured while executing the statement")
        } else {
            Ok(true)
        }
    }

    fn create_binds(cmd: &mut Command, binds: &mut Vec<Bind>) -> Result<usize> {
        let cols: i32 = cmd
            .res_info(CS_NUMDATA)
            .unwrap();
        binds.resize(cols as usize, Default::default());
        for col in 0..cols {
            /*
                bind.name for column alias
                bind.status & CS_CANBENULL
            */
            let bind = &mut binds[col as usize];
            bind.fmt = cmd.describe(col + 1).unwrap();
            bind.fmt.format = CS_FMT_UNUSED as i32;
            match bind.fmt.datatype {
                CS_CHAR_TYPE | CS_LONGCHAR_TYPE | CS_VARCHAR_TYPE | CS_UNICHAR_TYPE | CS_TEXT_TYPE | CS_UNITEXT_TYPE => {
                    bind.fmt.maxlength += 1;
                    bind.fmt.format = CS_FMT_NULLTERM as i32;
                },
                _ => {}
            }
            bind.buffer.resize(bind.fmt.maxlength as usize, 0);
            bind.fmt.count = 1;

            unsafe {
                cmd.bind_unsafe(
                    (col + 1) as i32,
                    &mut bind.fmt,
                    mem::transmute(bind.buffer.as_mut_ptr()),
                    &mut bind.data_length,
                    &mut bind.indicator)
                .unwrap();
            }
        }
        Ok(cols as usize)
    }

    fn get_row_result(&mut self) -> Result<bool> {
        match self.state {
            StatementState::Done => {
                return Ok(false)
            },
            StatementState::New | StatementState::ResultsReady => {
                if self.state == StatementState::ResultsReady {
                    self.command.cancel(CS_CANCEL_CURRENT)?;
                }

                loop {
                    let (ret, res_type) = self.command.results()?;
                    if ret {
                        match res_type {
                            CS_ROW_RESULT => {
                                self.state = StatementState::ResultsReady;
                                Self::create_binds(&mut self.command, &mut self.binds)?;
                                return Ok(true);
                            },
                            CS_COMPUTE_RESULT | CS_CURSOR_RESULT | CS_PARAM_RESULT | CS_STATUS_RESULT => {
                                self.command.cancel(CS_CANCEL_CURRENT)?;
                            },
                            CS_CMD_DONE => {
                                self.state = StatementState::Done;
                            },
                            CS_CMD_FAIL => {
                                self.has_errors = true;
                            },
                            _ => {
                                /* Do nothing, most notably, ignore CS_CMD_SUCCEED */
                            }
                        }
                    } else {
                        break;
                    }
                }
                return Ok(false);
            },
            _ => {
                return err!("Invalid statement state");
            }
        }
    }

    pub fn next(&mut self) -> Result<bool> {
        if self.state == StatementState::Done || self.state == StatementState::ResultSetDone {
            return Ok(false)
        } else if self.state == StatementState::New {
            return err!("Invalid statement state");
        }
        
        let ret = self.command.fetch()?;
        if !ret {
            self.state = StatementState::ResultSetDone;
            return Ok(false);
        }
        self.state = StatementState::ResultsReady;
        return Ok(true);
    }

    pub fn column_count(&self) -> Result<usize> {
        if self.state != StatementState::ResultsReady {
            err!("Invalid statement state")
        } else {
            Ok(self.binds.len())
        }
    }

    pub fn get_string(&mut self, col: impl TryInto<usize>) -> Result<String> {
        if self.state != StatementState::ResultsReady {
            return err!("Invalid statement state");
        }

        let col_index: usize = col.try_into()
            .map_err(|_| Error::from_message("Invalid column index"))?;
        let bind = &self.binds[col_index];
        match bind.fmt.datatype {
            CS_CHAR_TYPE | CS_LONGCHAR_TYPE | CS_VARCHAR_TYPE | CS_UNICHAR_TYPE | CS_TEXT_TYPE | CS_UNITEXT_TYPE => {
                let len = (bind.data_length as usize) - 1;
                let value = String::from_utf8_lossy(&bind.buffer.as_slice()[0..len]);
                return Ok(value.to_string());
            },
            _ => {
                let mut dstfmt: CS_DATAFMT = Default::default();
                dstfmt.datatype = CS_CHAR_TYPE;
                dstfmt.maxlength = if bind.fmt.maxlength < 200 { 100 } else { bind.fmt.maxlength * 2 };
                dstfmt.format = CS_FMT_NULLTERM as i32;
                dstfmt.count = 1;

                let mut dstdata: Vec<u8> = Vec::new();
                dstdata.resize(dstfmt.maxlength as usize, Default::default());
                let dstlen = self.command.conn.ctx.convert(
                    &bind.fmt, &bind.buffer,
                    &dstfmt,
                    &mut dstdata)?;

                return Ok(String::from_utf8_lossy(&dstdata.as_slice()[0..dstlen]).to_string());
            }
        }
    }

}
