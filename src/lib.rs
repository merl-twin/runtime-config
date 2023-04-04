use std::{
    fs,
    path::PathBuf,
    io::Read,
    collections::BTreeMap,
};



pub struct FileEvent<T> {
    pub opaque: T,
    pub content: FileContent,
}

pub enum FileContent {
    Init(String),
    Update(String),
    Remove,
    Error(std::io::Error),
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
    },
    Unwatch(PathBuf),
}

struct Watch<T> {
    opaque: T,
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
            WatchTask::Watch{ opaque, tp, path } => match self.tasks.get_mut(&path) {
                Some(tsk) => {
                    tsk.opaque = opaque;
                    tsk.tp = tp;
                },
                None => {
                    let (opt_w,ev) = FileWatcherInner::init(opaque,tp,&path);
                    if let Some(w) = opt_w {
                        self.tasks.insert(path,w);
                    }
                    self.sender.send(ev).ok();
                },
            },
            WatchTask::Unwatch(path) => {
                self.tasks.remove(&path);
            },
        }
    }

    fn init(opaque: T, tp: FileType, path: &PathBuf) -> (Option<Watch<T>>,FileEvent<T>) {
        fn path_to_file(path: &PathBuf) -> Result<(std::time::SystemTime,fs::File),std::io::Error> {
            let mtime = fs::metadata(path)?.modified()?;
            let fl = fs::File::open(path)?;
            Ok((mtime,fl))            
        }

        match path_to_file(path) {
            Err(e) => (None,FileEvent{ opaque, content: FileContent::Error(e) }),
            Ok((mtime,fl)) => {
                match tp {
                    FileType::Text => {
                        let mut rdr = std::io::BufReader::new(fl);
                        let mut s = String::new();
                        match rdr.read_to_string(&mut s) {
                            Ok(_) => (Some(Watch { opaque: opaque.clone(), tp, modified: mtime }),FileEvent{ opaque, content: FileContent::Init(s) }),
                            Err(e) => (None,FileEvent{ opaque, content: FileContent::Error(e) }),
                        }
                    },
                }
            },
        }
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
                                let event = match rdr.read_to_string(&mut s) {
                                    Ok(_) => {
                                        watch.modified = mtime;
                                        FileEvent{ opaque: watch.opaque.clone(), content: FileContent::Update(s) }
                                    },
                                    Err(e) => FileEvent{ opaque: watch.opaque.clone(), content: FileContent::Error(e) },
                                };
                                self.sender.send(event).ok();
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

    pub fn add_watch<P: Into<PathBuf>>(&self, tp: FileType, path: P, opaque: T) {
        let task = WatchTask::Watch {
            opaque,
            tp,
            path: path.into(),
        };
        if let Some(sender) = &self.sender {
            sender.send(task).ok();
        }
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
