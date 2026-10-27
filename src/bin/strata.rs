use std::io::{self, BufRead, Write};
use std::process::ExitCode;
use std::sync::Arc;

use strata::{Config, Database};

const DEFAULT_ADDR: &str = "127.0.0.1:7432";
const SHELL_MAX: usize = 32 * 1024 * 1024;

const USAGE: &str = "\
usage: strata <command> [options]
  init PATH [--password SECRET]
  shell PATH [--password SECRET]
  exec PATH --sql SQL [--password SECRET]
  serve PATH [--addr ADDR] [--password SECRET]
  check PATH [--password SECRET]
  backup PATH DEST [--password SECRET]
ADDR defaults to 127.0.0.1:7432";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(CliError::Usage) => {
            let _ = writeln!(io::stderr(), "{USAGE}");
            ExitCode::from(2)
        }
        Err(CliError::BrokenPipe) => ExitCode::SUCCESS,
        Err(CliError::Msg(msg)) => {
            let _ = writeln!(io::stderr(), "{msg}");
            ExitCode::from(1)
        }
    }
}

fn run() -> Result<(), CliError> {
    let mut args = std::env::args().skip(1);
    let Some(cmd) = args.next() else {
        return Err(CliError::Usage);
    };
    if cmd == "-h" || cmd == "--help" || cmd == "help" {
        return Err(CliError::Usage);
    }
    let rest: Vec<String> = args.collect();
    let flags = parse_flags(&rest)?;
    match cmd.as_str() {
        "init" => {
            expect(&flags, false, false, 1)?;
            cmd_init(&flags.positionals[0], flags.password)
        }
        "shell" => {
            expect(&flags, false, false, 1)?;
            cmd_shell(&flags.positionals[0], flags.password)
        }
        "exec" => {
            expect(&flags, true, false, 1)?;
            let sql = flags.sql.ok_or(CliError::Usage)?;
            cmd_exec(&flags.positionals[0], &sql, flags.password)
        }
        "serve" => {
            expect(&flags, false, true, 1)?;
            let addr = flags.addr.unwrap_or_else(|| DEFAULT_ADDR.to_string());
            cmd_serve(&flags.positionals[0], &addr, flags.password)
        }
        "check" => {
            expect(&flags, false, false, 1)?;
            cmd_check(&flags.positionals[0], flags.password)
        }
        "backup" => {
            expect(&flags, false, false, 2)?;
            cmd_backup(
                &flags.positionals[0],
                &flags.positionals[1],
                flags.password,
            )
        }
        _ => Err(CliError::Usage),
    }
}

fn cmd_init(path: &str, password: Option<String>) -> Result<(), CliError> {
    let db = Database::create(path, config(password))?;
    db.close()?;
    Ok(())
}

fn cmd_check(path: &str, password: Option<String>) -> Result<(), CliError> {
    let db = Database::open(path, config(password))?;
    write_stdout("ok")?;
    db.close()?;
    Ok(())
}

fn cmd_backup(path: &str, dest: &str, password: Option<String>) -> Result<(), CliError> {
    let db = Database::open(path, config(password))?;
    let copied = db.backup(dest);
    let closed = db.close();
    copied?;
    closed?;
    Ok(())
}

fn cmd_exec(path: &str, sql: &str, password: Option<String>) -> Result<(), CliError> {
    let db = Database::open(path, config(password))?;
    // exec is one-shot auto-commit. Shell and serve keep a session instead.
    let result = db.execute(sql);
    let closed = db.close();
    match result {
        Ok(value) => {
            let printed = write_stdout(&value.to_text());
            closed?;
            printed
        }
        Err(e) => {
            let _ = closed;
            Err(CliError::Msg(e.to_string()))
        }
    }
}

fn cmd_shell(path: &str, password: Option<String>) -> Result<(), CliError> {
    let db = Database::open(path, config(password))?;
    let mut session = db.session();
    let result = shell_loop(&db, &mut session);
    if session.txn_id.is_some() {
        let _ = db.rollback_session(&mut session);
    }
    let closed = db.close();
    match result {
        Ok(()) => closed.map_err(CliError::from),
        Err(e) => {
            let _ = closed;
            Err(e)
        }
    }
}

fn shell_loop(db: &Database, session: &mut strata::Session) -> Result<(), CliError> {
    let stdin = io::stdin();
    let mut lines = stdin.lock().lines();
    let mut pending = String::new();
    loop {
        let line = match lines.next() {
            None => break,
            Some(Ok(line)) => line,
            Some(Err(e)) => return Err(CliError::Msg(e.to_string())),
        };
        if line.trim() == ".quit" {
            break;
        }
        if pending.len().saturating_add(line.len()).saturating_add(1) > SHELL_MAX {
            eprint_line("statement exceeds 33554432 bytes");
            pending.clear();
            continue;
        }
        if !pending.is_empty() {
            pending.push('\n');
        }
        pending.push_str(&line);
        if line.contains(';') {
            let sql = std::mem::take(&mut pending);
            match db.execute_session(session, &sql) {
                Ok(result) => write_stdout(&result.to_text())?,
                Err(e) => eprint_line(&e.to_string()),
            }
        }
    }
    Ok(())
}

fn cmd_serve(path: &str, addr: &str, password: Option<String>) -> Result<(), CliError> {
    let db = Arc::new(Database::open(path, config(password.clone()))?);
    strata::server::serve(db, addr, password)?;
    Ok(())
}

fn config(password: Option<String>) -> Config {
    let mut cfg = Config::default();
    cfg.buffer_pages = 128;
    cfg.password = password;
    cfg
}

struct Flags {
    positionals: Vec<String>,
    sql: Option<String>,
    addr: Option<String>,
    password: Option<String>,
}

fn parse_flags(args: &[String]) -> Result<Flags, CliError> {
    let mut flags = Flags {
        positionals: Vec::new(),
        sql: None,
        addr: None,
        password: None,
    };
    let mut i = 0;
    let mut ended = false;
    while i < args.len() {
        let arg = &args[i];
        if !ended && arg == "--" {
            ended = true;
            i += 1;
            continue;
        }
        if !ended && arg.starts_with("--") {
            let (name, inline) = split_opt(arg);
            let value = if let Some(v) = inline {
                v
            } else {
                i += 1;
                if i >= args.len() {
                    return Err(CliError::Usage);
                }
                args[i].clone()
            };
            match name {
                "--sql" => assign(&mut flags.sql, value)?,
                "--addr" => assign(&mut flags.addr, value)?,
                "--password" => assign(&mut flags.password, value)?,
                _ => return Err(CliError::Usage),
            }
        } else if !ended && arg.starts_with('-') {
            return Err(CliError::Usage);
        } else {
            flags.positionals.push(arg.clone());
        }
        i += 1;
    }
    Ok(flags)
}

fn split_opt(arg: &str) -> (&str, Option<String>) {
    if let Some((name, value)) = arg.split_once('=') {
        if name.starts_with("--") && !name[2..].is_empty() {
            return (name, Some(value.to_string()));
        }
    }
    (arg, None)
}

fn assign(slot: &mut Option<String>, value: String) -> Result<(), CliError> {
    if slot.is_some() {
        return Err(CliError::Usage);
    }
    *slot = Some(value);
    Ok(())
}

fn expect(flags: &Flags, sql: bool, addr: bool, positionals: usize) -> Result<(), CliError> {
    if flags.positionals.len() != positionals {
        return Err(CliError::Usage);
    }
    if !sql && flags.sql.is_some() {
        return Err(CliError::Usage);
    }
    if sql && flags.sql.is_none() {
        return Err(CliError::Usage);
    }
    if !addr && flags.addr.is_some() {
        return Err(CliError::Usage);
    }
    Ok(())
}

enum CliError {
    Usage,
    BrokenPipe,
    Msg(String),
}

impl From<strata::Error> for CliError {
    fn from(err: strata::Error) -> Self {
        CliError::Msg(err.to_string())
    }
}

fn write_stdout(text: &str) -> Result<(), CliError> {
    let mut out = io::stdout().lock();
    match writeln!(out, "{text}").and_then(|_| out.flush()) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::BrokenPipe => Err(CliError::BrokenPipe),
        Err(e) => Err(CliError::Msg(e.to_string())),
    }
}

fn eprint_line(text: &str) {
    let mut err = io::stderr().lock();
    let _ = writeln!(err, "{text}");
}
