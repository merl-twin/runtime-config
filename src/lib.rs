use std::{
    fs,
    path::PathBuf,
    io::Read,
    collections::BTreeMap,
};

use oneshot::{OneSet,OneGet};

pub struct FileEvent<T> {
    pub opaque: T,
    pub content: FileContent,
}

#[derive(Debug,Clone)]
pub enum FileContent {
    Text(String),
    Remove,
    Error(String),
}
impl From<std::io::Error> for FileContent {
    fn from(e: std::io::Error) -> FileContent {
        FileContent::Error(e.to_string())
    }
}

#[derive(Clone,Copy)]
pub enum FileType {
    Text,
}

enum WatchTask<T> {
    Watch {
        opaque: T,
        tp: FileType,
        path: PathBuf,
        result: OneSet<FileContent>,
    },
    Unwatch(PathBuf),
}

struct Watch<T> {
    opaque: Vec<T>,
    tp: FileType,
    modified: std::time::SystemTime,
}


#[derive(Debug)]
pub enum TryError {
    Empty,
    Closed,
}

pub struct FileWatcher<T: Clone + Send + 'static> {
    sender: Option<crossbeam::channel::Sender<WatchTask<T>>>,
    receiver: crossbeam::channel::Receiver<FileEvent<T>>,
    handle: Option<std::thread::JoinHandle<()>>,
}


impl<T: Clone + Send + 'static> FileWatcher<T> {
    pub fn new() -> FileWatcher<T> {
        let (tx,rx) = crossbeam::channel::unbounded();
        let (itx,irx) = crossbeam::channel::unbounded();

        FileWatcher {
            sender: Some(itx),
            receiver: rx,
            handle: Some(std::thread::Builder::new().name("file-watcher".to_string()).spawn(move || {
                let mut inner = FileWatcherInner::new(tx);
                loop {
                    match irx.recv_timeout(inner.timeout) {
                        Ok(task) => {
                            inner.task(task);
                            while let Ok(task) = irx.try_recv() {
                                inner.task(task);
                            }
                        },
                        Err(crossbeam::channel::RecvTimeoutError::Disconnected) => break,
                        Err(crossbeam::channel::RecvTimeoutError::Timeout) => {},
                    }

                    // Process
                    inner.check();
                }
            }).unwrap()),
        }
    }

    pub fn add_watch<P: Into<PathBuf>>(&self, tp: FileType, path: P, opaque: T) -> OneGet<FileContent> {
        let (os,og) = oneshot::oneshot();
        let task = WatchTask::Watch {
            opaque,
            tp,
            path: path.into(),
            result: os,
        };
        if let Some(sender) = &self.sender {
            sender.send(task).ok();
        }
        og
    }

    pub fn remove_watch<P: Into<PathBuf>>(&self, path: P) {
        let task = WatchTask::Unwatch(path.into());
        if let Some(sender) = &self.sender {
            sender.send(task).ok();
        }
    }

    pub fn try_recv(&self, timeout: Option<std::time::Duration>) -> Result<FileEvent<T>,TryError> {
        match timeout {
            Some(to) => self.receiver.recv_timeout(to).map_err(|e| match e {
                crossbeam::channel::RecvTimeoutError::Disconnected => TryError::Closed,
                crossbeam::channel::RecvTimeoutError::Timeout => TryError::Empty,
            }),
            None => self.receiver.try_recv().map_err(|e| match e {
                crossbeam::channel::TryRecvError::Disconnected => TryError::Closed,
                crossbeam::channel::TryRecvError::Empty => TryError::Empty,
            }),
        }
    }
}

impl<T: Clone + Send + 'static> Drop for FileWatcher<T> {
    fn drop(&mut self) {
        log::info!("Terminating file-watcher");
        if let Some(sender) = self.sender.take() {
            std::mem::drop(sender);
        }
        if let Some(handle) = self.handle.take() {
            if let Err(_) = handle.join() {
                log::warn!("file-watcher joininig failed");
            }
        }
        log::info!("Terminated file-watcher");
    }
}

struct FileWatcherInner<T: Clone> {
    sender: crossbeam::channel::Sender<FileEvent<T>>,
    tasks: BTreeMap<PathBuf,Watch<T>>,
    timeout: std::time::Duration,
}
impl<T: Clone> FileWatcherInner<T> {
    fn new(sender: crossbeam::channel::Sender<FileEvent<T>>) -> FileWatcherInner<T>
    {
        FileWatcherInner {
            sender,
            timeout: std::time::Duration::new(1,0),
            tasks: BTreeMap::new(),
        }
    }
    fn task(&mut self, task: WatchTask<T>) {
        match task {
            WatchTask::Watch{ opaque, tp, path, result } => match self.tasks.get_mut(&path) {
                Some(tsk) => {
                    tsk.opaque.push(opaque);
                    let c = match fs::File::open(path) {
                        Ok(fl) => match tp {
                            FileType::Text => {
                                let mut rdr = std::io::BufReader::new(fl);
                                let mut s = String::new();
                                match rdr.read_to_string(&mut s) {
                                    Ok(_) => FileContent::Text(s),
                                    Err(e) => e.into(),
                                }
                            },
                        },
                        Err(e) => e.into(),
                    };
                    result.set(c);
                },
                None => {
                    if let Some(w) = FileWatcherInner::init(opaque,tp,result,&path) {
                        self.tasks.insert(path,w);
                    }
                },
            },
            WatchTask::Unwatch(path) => {
                self.tasks.remove(&path);
            },
        }
    }

    fn init(opaque: T, tp: FileType, result: OneSet<FileContent>, path: &PathBuf) -> Option<Watch<T>> {
        fn path_to_file(path: &PathBuf) -> Result<(std::time::SystemTime,fs::File),std::io::Error> {
            let mtime = fs::metadata(path)?.modified()?;
            let fl = fs::File::open(path)?;
            Ok((mtime,fl))            
        }

        let (w,c) = match path_to_file(path) {
            Err(e) => (None,e.into()),
            Ok((mtime,fl)) => {
                match tp {
                    FileType::Text => {
                        let mut rdr = std::io::BufReader::new(fl);
                        let mut s = String::new();
                        match rdr.read_to_string(&mut s) {
                            Ok(_) => (Some(Watch { opaque: vec![opaque], tp, modified: mtime }),FileContent::Text(s)),
                            Err(e) => (None,e.into()),
                        }
                    },
                }
            },
        };
        result.set(c);
        w
    }

    fn check(&mut self) {
        fn path_to_meta(path: &PathBuf) -> Result<std::time::SystemTime,std::io::Error> {
            fs::metadata(path)?.modified()         
        }
        
        let mut to_remove = Vec::new();
        for (path,watch) in &mut self.tasks {
            match path_to_meta(path) {
                Ok(mtime) => match mtime > watch.modified {
                    true => match fs::File::open(path) {
                        Ok(fl) => match watch.tp {
                            FileType::Text => {
                                let mut rdr = std::io::BufReader::new(fl);
                                let mut s = String::new();
                                let event_cont = match rdr.read_to_string(&mut s) {
                                    Ok(_) => {
                                        watch.modified = mtime;                                        
                                        FileContent::Text(s)
                                    },
                                    Err(e) => e.into(),
                                };
                                for op in &watch.opaque {                                    
                                    self.sender.send(FileEvent{ opaque: op.clone(), content: event_cont.clone() }).ok();
                                }
                            },
                        },
                        Err(_) => to_remove.push(path.clone()),
                    },
                    false => continue,
                },
                Err(_) => to_remove.push(path.clone()),
            }
        }
        for path in to_remove {
            self.tasks.remove(&path);
        }
    }
}
