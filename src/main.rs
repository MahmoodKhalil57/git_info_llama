extern crate git2;
extern crate rusqlite;

use git2::{Commit, Reference, Repository};
use rusqlite::{params, Connection, Result};
use std::env;
use std::fs;

fn main() {
    let db_path = "example.db";
    let db_exists = fs::metadata(db_path).is_ok();
    let mut conn = Connection::open(db_path).expect("Failed to open database");

    // Check if the database file exists
    if !db_exists {
        // Call the create_database function to initialize your database tables.
        match create_database(&conn) {
            Ok(_) => println!("Database and tables created successfully!"),
            Err(e) => eprintln!("Error: {}", e),
        }
    }

    let mut commits: Vec<CommitDetails> = Vec::new();
    get_commits_detail_array(&mut commits);
    let _ = batch_insert_commits(&mut conn, &commits);

    let mut refs: Vec<RefDetails> = Vec::new();
    get_ref_details(&mut refs);
    let _ = batch_insert_refs(&mut conn, &refs);
}

struct CommitDetails {
    id: String,
    author: String,
    date: i64, // UNIX timestamp for simplicity, but can use a more detailed type if desired.
    message: String,
}
struct RefDetails {
    name: String,
    id: String,
    kind: String,
}

fn create_database(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute(
        "CREATE TABLE commit_details (
            id TEXT PRIMARY KEY,
            author TEXT NOT NULL,
            date INTEGER NOT NULL,
            message TEXT NOT NULL
        )",
        {},
    )?;

    conn.execute(
        "CREATE TABLE ref_details (
            name TEXT NOT NULL,
            id TEXT NOT NULL,
            kind TEXT NOT NULL,
            PRIMARY KEY (name, id)
        )",
        {},
    )?;

    Ok(())
}

fn get_commits_detail_array(commits: &mut Vec<CommitDetails>) {
    // Get the current directory.
    let path = env::current_dir().unwrap();

    let repo = Repository::open(&path).expect("Failed to open the repository.");

    // Create an iterator over all the commits.
    let mut revwalk = repo.revwalk().expect("Failed to get revwalk.");
    revwalk.push_head().expect("Failed to push head.");

    // Iterate over all the commits and print them.
    for oid in revwalk {
        match oid {
            Ok(oid) => {
                let commit = repo.find_commit(oid).expect("Failed to find commit.");
                let formatted_commit = extract_commit_details(&commit);
                commits.push(formatted_commit);
            }
            Err(e) => println!("Failed to process commit: {}", e),
        }
    }
}

fn get_ref_details(refs: &mut Vec<RefDetails>) {
    let path = env::current_dir().unwrap();
    let repo = Repository::open(&path).expect("Failed to open the repository.");

    for reference in repo.references().expect("Failed to get references.") {
        match reference {
            Ok(reference) => {
                let formatted_refs = extract_ref_details(&reference);
                refs.push(formatted_refs);
            }
            Err(e) => println!("Failed to process reference: {}", e),
        }
    }
}

fn extract_commit_details(commit: &Commit) -> CommitDetails {
    let id = commit.id().to_string();
    let author = commit.author().name().unwrap_or("Unknown").to_string();
    let date = commit.time().seconds();
    let message = commit.message().unwrap_or("No message").to_string();

    return CommitDetails {
        id,
        author,
        date,
        message,
    };
}

fn extract_ref_details(reference: &Reference) -> RefDetails {
    let name = reference.name().unwrap_or("").to_string();
    let id = match reference.target() {
        Some(target) => target.to_string(),
        None => String::from("Unknown"),
    };
    let kind = match reference.kind() {
        Some(git2::ReferenceType::Direct) => "Direct",
        Some(git2::ReferenceType::Symbolic) => "Symbolic",
        None => "Unknown",
    }
    .to_string();

    return RefDetails { id, name, kind };
}

fn batch_insert_commits(conn: &mut Connection, commits: &Vec<CommitDetails>) -> Result<()> {
    let chunk_size = 50;

    let insert_sql =
        "INSERT INTO commit_details (id, author, date, message) VALUES (?1, ?2, ?3, ?4)";

    for chunk in commits.chunks(chunk_size) {
        let tx = conn.transaction()?; // Begin a new transaction

        for commit in chunk {
            tx.execute(
                insert_sql,
                params![&commit.id, &commit.author, commit.date, &commit.message],
            )?;
        }
        println!("Committing");
        tx.commit()?; // Commit the transaction
    }

    Ok(())
}

fn batch_insert_refs(conn: &mut Connection, refs: &Vec<RefDetails>) -> Result<()> {
    let chunk_size = 50;

    let insert_sql = "INSERT INTO ref_details (id, name, kind) VALUES (?1, ?2, ?3)";

    for chunk in refs.chunks(chunk_size) {
        let tx = conn.transaction()?; // Begin a new transaction

        for reference in chunk {
            tx.execute(
                insert_sql,
                params![&reference.id, &reference.name, reference.kind,],
            )?;
        }

        println!("Committing");
        tx.commit()?; // Commit the transaction
    }

    Ok(())
}
