using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using static FileAgeRetainer.CommonComponents;

namespace FileAgeRetainer
{
    /// <summary>
    /// queue watcher for a retainer item, processes enqueued events from associated file system watcher
    /// </summary>
    internal class QueueWatcher
    {
        private string _RootConfigItem;
        private int _RetainersListIndex;
        private bool _QueueProcessorIgnoreChangedWhenCreatedThisBatch;
        private List<string> _CreatedItemsThisBatch;
        private bool _FsTypeChangeCausePreservationReset;
        private bool _FsChecksumChangeCausePreservationReset;
        private bool _FsRenameCausePreservationReset;
        private FsHashAlgorithm _FsHashAlgorithm;
        private bool _FsHashCalculateWholeDirectoryHashes;

        /// <summary>
        /// true if should ignore change events when item created in this worker operation cycle
        /// </summary>
        public bool QueueProcessorIgnoreChangedWhenCreatedThisBatch { get { return _QueueProcessorIgnoreChangedWhenCreatedThisBatch; } }

        /// <summary>
        /// root config item for this queue watcher
        /// </summary>
        public string RootConfigItem { get { return _RootConfigItem; } }

        /// <summary>
        /// position in list where this queue watchers retainer item is located - need to find dictionary etc.
        /// </summary>
        public int RetainersListIndex { get { return _RetainersListIndex; } }

        /// <summary>
        /// true if type change causes preservation time reset
        /// </summary>
        public bool FsTypeChangeCausePreservationReset { get { return _FsTypeChangeCausePreservationReset; } }

        /// <summary>
        /// trye if checksum change causes preservation time reset
        /// </summary>
        public bool FsChecksumChangeCausePreservationReset { get { return _FsChecksumChangeCausePreservationReset; } }

        /// <summary>
        /// true if rename causes preservation reset
        /// </summary>
        public bool FsRenameCausePreservationReset { get { return _FsRenameCausePreservationReset; } }

        /// <summary>
        /// hash algorithm for this queue watcher
        /// </summary>
        public FsHashAlgorithm FsHashAlgorithm { get { return _FsHashAlgorithm; } }

        /// <summary>
        /// true if whole directory hashes should be created - EXPENSIVE
        /// </summary>
        public bool FsHashCalculateWholeDirectoryHashes { get { return _FsHashCalculateWholeDirectoryHashes; } }

        /// <summary>
        /// instantiate QueueWatcher instance
        /// </summary>
        /// <param name="RootConfigItem">root config item</param>
        /// <param name="QueueProcessorIgnoreChangedWhenCreatedThisBatch">ignore changes when fs object created in this opertation cycle of queue watcher</param>
        /// <param name="FsTypeChangeCausePreservationReset">true if type change causes preservation time reset</param>
        /// <param name="FsChecksumChangeCausePreservationReset">true if file system object changes cause preservation time reset</param>
        /// <param name="FsRenameCausePreservationReset">true if renamed fs objects cause preservation time reset</param>
        /// <param name="hashAlgorithm">hash algorithm in use for this queue watcher</param>
        /// <param name="FsHashCalculateWholeDirectoryHashes">true if should calculate full directory subtree hashes on directories - EXPENSIVE</param>
        public QueueWatcher(string RootConfigItem, bool QueueProcessorIgnoreChangedWhenCreatedThisBatch, bool FsTypeChangeCausePreservationReset, bool FsChecksumChangeCausePreservationReset, bool FsRenameCausePreservationReset, FsHashAlgorithm hashAlgorithm, bool FsHashCalculateWholeDirectoryHashes)
        {
            _RetainersListIndex = FileAgeRetainer.retainers.FindIndex(item => String.Compare(item.RootConfigItem, RootConfigItem, 0) == 0);
            _RootConfigItem = RootConfigItem;
            _QueueProcessorIgnoreChangedWhenCreatedThisBatch = QueueProcessorIgnoreChangedWhenCreatedThisBatch;
            _FsTypeChangeCausePreservationReset = FsTypeChangeCausePreservationReset;
            _FsChecksumChangeCausePreservationReset = FsChecksumChangeCausePreservationReset;
            _FsRenameCausePreservationReset = FsRenameCausePreservationReset;
            _FsHashAlgorithm = hashAlgorithm;
            _FsHashCalculateWholeDirectoryHashes = FsHashCalculateWholeDirectoryHashes;
        }

        /// <summary>
        /// process queues for the associated retainer item
        /// </summary>
        /// <param name="state">state metadata used for auto reset</param>
        internal void ProcessQueues(object state)
        {
            AutoResetEvent autoEvent = (AutoResetEvent)state;

            if (FileAgeRetainer.RetainersInitializing)
            {
                DiagnosticsEventHandler.LogEvent(400, "queue processor " + _RootConfigItem + " ...cannot work, retainers initializing...", EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
                autoEvent.Set();
                return;
            }

            //determine where we are in retainers list
            if (_RetainersListIndex == -1)
            {
                _RetainersListIndex = FileAgeRetainer.retainers.FindIndex(item => String.Compare(item.RootConfigItem, RootConfigItem, 0) == 0);
            }
            if (_RetainersListIndex == -1)
            {
                DiagnosticsEventHandler.LogEvent(401, "queue processor " + _RootConfigItem + " ...cannot work, cannot find retainer...", EventLogEntryType.Error, DiagnosticsEventHandler.OutputChannels.DebugConsole);
                autoEvent.Set();
                return;
            }

            //safe to enter critical section
            if (!Monitor.TryEnter(FileAgeRetainer.retainers[_RetainersListIndex].QueueWatchTimerLock))
            {
                DiagnosticsEventHandler.LogEvent(402, "queue processor " + _RootConfigItem + " ...cannot work, already in timer critical section...", EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
                autoEvent.Set();
                return;
            }

#if DEBUG
            DiagnosticsEventHandler.LogEvent(403, "queue processor " + _RootConfigItem + " ...woke to do work...", EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
#endif
            Stopwatch workTimer = new Stopwatch();
            workTimer.Start();

            _CreatedItemsThisBatch = new List<string>();

            if (FileAgeRetainer.retainers[_RetainersListIndex]._fsCreatedItemQueue.Count > 0) ProcessAllCreatedEvents();
            if (FileAgeRetainer.retainers[_RetainersListIndex]._fsChangedItemQueue.Count > 0) ProcessAllChangedEvents();
            if (FileAgeRetainer.retainers[_RetainersListIndex]._fsRenamedItemQueue.Count > 0) ProcessAllRenamedEvents();
            if (FileAgeRetainer.retainers[_RetainersListIndex]._fsDeletedItemQueue.Count > 0) ProcessAllDeletedEvents();

            workTimer.Stop();

#if DEBUG
            DiagnosticsEventHandler.LogEvent(404, "queue processor " + _RootConfigItem + " ...job's done... " + workTimer.ElapsedMilliseconds + "ms", EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
#endif

            //we're done - let others in
            Monitor.Exit(FileAgeRetainer.retainers[_RetainersListIndex].QueueWatchTimerLock);

            autoEvent.Set();
        }

        private void ProcessAllDeletedEvents()
        {
#if DEBUG
            DiagnosticsEventHandler.LogEvent(405, "queue processor " + _RootConfigItem + "DELETED queue items: " + FileAgeRetainer.retainers[_RetainersListIndex]._fsDeletedItemQueue.Count, EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
#endif
            while (FileAgeRetainer.retainers[_RetainersListIndex]._fsDeletedItemQueue.Count > 0)
            {
                FileSystemEventArgs deletedArgs = null;
                FileAgeRetainer.retainers[_RetainersListIndex]._fsDeletedItemQueue.TryDequeue(out deletedArgs);
                if (deletedArgs != null)
                {
                    ProcessDeletedEvent(deletedArgs);
                }
            }

        }

        /// <summary>
        /// process an event that fired due to fs object deletion
        /// </summary>
        /// <param name="e">the event that occured</param>
        private void ProcessDeletedEvent(FileSystemEventArgs e)
        {
            CachedFsInfo item = null;

            FileAgeRetainer.retainers[_RetainersListIndex].fsCachedInfoDictionary.TryRemove(e.FullPath, out item);

#if DEBUG
            if (item == null)
            {
                DiagnosticsEventHandler.LogEvent(406, "file " + e.FullPath + " could NOT be removed from dictionary", EventLogEntryType.Warning);
            }
            else
            {
                DiagnosticsEventHandler.LogEvent(407, "file " + e.FullPath + " deleted", EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
            }
#endif
        }

        /// <summary>
        /// process all renamed events in associated retainer item queue
        /// </summary>
        private void ProcessAllRenamedEvents()
        {
#if DEBUG
            DiagnosticsEventHandler.LogEvent(408, "queue processor " + _RootConfigItem + "RENAMED queue items: " + FileAgeRetainer.retainers[_RetainersListIndex]._fsRenamedItemQueue.Count, EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
#endif

            List<RenamedEventArgs> tempRenamedList = new List<RenamedEventArgs>();

            while (FileAgeRetainer.retainers[_RetainersListIndex]._fsRenamedItemQueue.Count > 0)
            {
                RenamedEventArgs renamedArgs = null;
                FileAgeRetainer.retainers[_RetainersListIndex]._fsRenamedItemQueue.TryDequeue(out renamedArgs);
                if (renamedArgs != null)
                {
                    tempRenamedList.Add(renamedArgs);
                }
            }

            Parallel.ForEach<RenamedEventArgs>(tempRenamedList, CommonComponents.ParallelOptions, item =>
            {
                ProcessRenamedEvent(item);
            });

        }

        /// <summary>
        /// process a renamed event
        /// </summary>
        /// <param name="e">the event that occured</param>
        private void ProcessRenamedEvent(RenamedEventArgs e)
        {

            FsObjType itemType = GetObjType(e.FullPath);

            switch (itemType)
            {
                case FsObjType.File:
                    RenameCachedFsItem(e);
                    break;
                case FsObjType.Directory:
                    RenameCachedFsSubtree(e);
                    break;
                case FsObjType.NotExist:
                    DiagnosticsEventHandler.LogEvent(409, "ERROR - TARGET FS ITEM FOR RENAME NOT EXIST - file " + e.OldFullPath + " renamed to " + e.FullPath + " FAILED", EventLogEntryType.Error);
                    break;
                default:
                    DiagnosticsEventHandler.LogEvent(410, "ERROR - BAD LOGIC IN ProcessRenamedEvent() - file " + e.OldFullPath + " renamed to " + e.FullPath + " FAILED", EventLogEntryType.Error);
                    break;
            }
        }

        /// <summary>
        /// process renaming of a cache file system subtree on directory rename - only rename event that will fire is for root of rename folder - so need to update all cached entries that were under old path
        /// </summary>
        /// <param name="e">the event that occured</param>
        private void RenameCachedFsSubtree(RenamedEventArgs e)
        {
            CachedFsInfo item;
            FileAgeRetainer.retainers[_RetainersListIndex].fsCachedInfoDictionary.TryRemove(e.OldFullPath, out item);
            if (item != null)
            {
                //there was an old item in dictionary, its been removed
                item.FullPath = e.FullPath;
                item.SignalRenamed();
                FileAgeRetainer.retainers[_RetainersListIndex].fsCachedInfoDictionary.TryAdd(item.FullPath, item);
#if DEBUG
                DiagnosticsEventHandler.LogEvent(411, "RENAMED - Directory " + e.OldFullPath + " renamed to " + e.FullPath, EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
#endif
                var matchingKeys = FileAgeRetainer.retainers[_RetainersListIndex].fsCachedInfoDictionary.Keys.Where(x => x.Contains(e.OldFullPath));

                Parallel.ForEach<string>(matchingKeys, CommonComponents.ParallelOptions, fsExistingKey =>
                {
                    RenameCachedFsItem(fsExistingKey, fsExistingKey.Replace(e.OldFullPath, e.FullPath));
                });
            }
            else
            {
                //for some reason there is no entry in dictionary - add.
                ProcessCreatedEvent(e.FullPath);
            }
        }

        /// <summary>
        /// rename a cached event item based on the event data
        /// </summary>
        /// <param name="e">the event that occured</param>
        private void RenameCachedFsItem(RenamedEventArgs e)
        {
            RenameCachedFsItem(e.OldFullPath, e.FullPath);
        }

        /// <summary>
        /// rename a cached event item based on passed parameters
        /// </summary>
        /// <param name="oldFullPath">old full path - the existing key in dictionary</param>
        /// <param name="fullPath">new full path - the new key in dictionary</param>
        private void RenameCachedFsItem(string oldFullPath, string fullPath)
        {
            CachedFsInfo item;
            FileAgeRetainer.retainers[_RetainersListIndex].fsCachedInfoDictionary.TryRemove(oldFullPath, out item);
            if (item != null)
            {
                //there was an old item in dictionary, its been removed
                item.FullPath = fullPath;
                item.SignalRenamed();
                FileAgeRetainer.retainers[_RetainersListIndex].fsCachedInfoDictionary.TryAdd(item.FullPath, item);
#if DEBUG
                DiagnosticsEventHandler.LogEvent(412, "RENAMED - file " + oldFullPath + " renamed to " + fullPath, EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
#endif
            }
            else
            {
                //for some reason there is no entry in dictionary - add.
                ProcessCreatedEvent(fullPath);
            }
        }

        /// <summary>
        /// process all created events in associated retainer item queue
        /// </summary>
        private void ProcessAllCreatedEvents()
        {
            //pull stuff into a temporary list
            List<FileSystemEventArgs> tempCreateList = new List<FileSystemEventArgs>();

            while (FileAgeRetainer.retainers[_RetainersListIndex]._fsCreatedItemQueue.Count > 0)
            {
                FileSystemEventArgs createdArgs = null;
                FileAgeRetainer.retainers[_RetainersListIndex]._fsCreatedItemQueue.TryDequeue(out createdArgs);
                if (createdArgs != null)
                {
                    tempCreateList.Add(createdArgs);
                    _CreatedItemsThisBatch.Add(createdArgs.FullPath);
                }
            }

#if DEBUG
            DiagnosticsEventHandler.LogEvent(413, "queue processor " + _RootConfigItem + "CREATED queue items processing in this batch: " + tempCreateList.Count, EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
#endif

            Parallel.ForEach<FileSystemEventArgs>(tempCreateList, CommonComponents.ParallelOptions, item =>
              {
                  ProcessCreatedEvent(item);
              });
        }

        /// <summary>
        /// process a created item event
        /// </summary>
        /// <param name="e">the event that occured</param>
        private void ProcessCreatedEvent(FileSystemEventArgs e)
        {
            ProcessCreatedEvent(e.FullPath);
        }

        private void ProcessCreatedEvent(string fullPath)
        {
            CachedFsInfo newItem = new CachedFsInfo(fullPath, _FsTypeChangeCausePreservationReset, _FsChecksumChangeCausePreservationReset, _FsRenameCausePreservationReset, _FsHashAlgorithm, _FsHashCalculateWholeDirectoryHashes);


            if (newItem.Type == FsObjType.NotExist)
            {
#if DEBUG
                DiagnosticsEventHandler.LogEvent(414, "file: " + fullPath + " requested to be added to cache but does not exist, skipping", EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
#endif
            }
            else
            {

                FileAgeRetainer.retainers[_RetainersListIndex].fsCachedInfoDictionary.AddOrUpdate(
                newItem.FullPath,
                newItem,
                (key, oldValue) => newItem);


#if DEBUG
                DiagnosticsEventHandler.LogEvent(415, "file: " + fullPath + " added to cache. checksum: " + newItem.HashString, EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
#endif
            }

        }

        /// <summary>
        /// process all changed events in the associated retainer item queue
        /// </summary>
        void ProcessAllChangedEvents()
        {
            //pull stuff into a temporary list
            List<FileSystemEventArgs> tempChangedList = new List<FileSystemEventArgs>();
            while (FileAgeRetainer.retainers[_RetainersListIndex]._fsChangedItemQueue.Count > 0)
            {
                FileSystemEventArgs changedArgs = null;
                FileAgeRetainer.retainers[_RetainersListIndex]._fsChangedItemQueue.TryDequeue(out changedArgs);
                if (changedArgs != null)
                {
                    tempChangedList.Add(changedArgs);
                }
            }

#if DEBUG
            DiagnosticsEventHandler.LogEvent(416, "queue processor " + _RootConfigItem + "CHANGED items to process in this batch: " + tempChangedList.Count, EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
#endif

            var distinctItems = tempChangedList.GroupBy(x => x.FullPath).Select(y => y.First());

#if DEBUG
            DiagnosticsEventHandler.LogEvent(417, "queue processor " + _RootConfigItem + "CHANGED DISTINCT items to process in this batch: " + distinctItems.Count(), EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
#endif

#if DEBUG
            DiagnosticsEventHandler.LogEvent(418, "queue processor " + _RootConfigItem + "IGNORE count as created this batch: " + (tempChangedList.Count - _CreatedItemsThisBatch.Count), EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
#endif

            Parallel.ForEach<FileSystemEventArgs>(distinctItems, CommonComponents.ParallelOptions, item =>
            {
                if (_QueueProcessorIgnoreChangedWhenCreatedThisBatch)
                {
                    if (!_CreatedItemsThisBatch.Contains(item.FullPath))
                    {
                        ProcessChangedEvent(item);
                    }
                }
                else
                {
                    ProcessChangedEvent(item);
                }
            });

            //clear this so not continually ignore.
            lock (_CreatedItemsThisBatch)
            {
                _CreatedItemsThisBatch.Clear();
            }
        }

        /// <summary>
        /// process a changed item event
        /// </summary>
        /// <param name="e">the event that occured</param>
        void ProcessChangedEvent(FileSystemEventArgs e)
        {
            //getting the checksum on the item in question is enough to reset the last seen timer if config option FsTypeChangeCausePreservationReset is true and object type changes
            //ie. folder became a file...  dunno if even possible as atomic operation, doubt it

            CachedFsInfo item;

            FileAgeRetainer.retainers[_RetainersListIndex].fsCachedInfoDictionary.TryGetValue(e.FullPath, out item);

            if (item == null)
            {
#if DEBUG
                DiagnosticsEventHandler.LogEvent(419, "file: " + e.FullPath + " could not be found in dictionary... file being written to disk right now?", EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
#endif
                if (GetObjType(e.FullPath) != FsObjType.NotExist)
                {
                    FileAgeRetainer.retainers[_RetainersListIndex]._fsChangedItemQueue.Enqueue(e);
                }
                return;
            }

            if (item.Type == FsObjType.NotExist)
            {
#if DEBUG
                DiagnosticsEventHandler.LogEvent(420, "file: " + e.FullPath + " is of type NotExist - this change event echo of delete or other activity?", EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
#endif
                return;
            }

            string originalHash = item.HashString;

            item.SignalChanged();



            FileAgeRetainer.retainers[_RetainersListIndex].fsCachedInfoDictionary.AddOrUpdate(
              item.FullPath,
              item,
              (key, oldValue) => item);

#if DEBUG
            DiagnosticsEventHandler.LogEvent(421, "file: " + e.FullPath + " changed. before: " + originalHash + " new: " + item.HashString, EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
#endif
        }
    }
}
