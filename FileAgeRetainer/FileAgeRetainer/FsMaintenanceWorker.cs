using Newtonsoft.Json;
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
    /// File System Maintenance Worker - responsible for implementing aging actions such as deletions, compression (when implemented) etc.  will directly update corresponding retainer item dictionary directly on occasion
    /// </summary>
    internal class FsMaintenanceWorker
    {
        private string _RootConfigItem;
        private string _FsWatchRoot;
        private string _Filter;
        private float _RetainMonitoredObjectsMinutes;
        private int _RetainersListIndex;
        private int _MaintenanceFrequencyMs;
        private FsObjType _RootType;
        private bool _FsMaintenanceAllowPerItemAging;
        private int _FsMaintenancePreservationDepth;
        private int _FsRootDepth;
        private bool _FsMaintenanceNeverDeleteDirectories;
        private bool _FsMaintenanceResetExpiredDirectoriesIfNeverDelete;
        private bool _FsMaintenanceOnlyDeleteFolderContentsAsGroup;

        /// <summary>
        /// root config item for this maintenance worker
        /// </summary>
        public string RootConfigItem { get { return _RootConfigItem; } }

        /// <summary>
        /// file system watch root for this maintenance worker
        /// </summary>
        public string FsWatchRoot { get { return _FsWatchRoot; } }

        /// <summary>
        /// filter for the maintenance worker watcher
        /// </summary>
        public string Filter { get { return _Filter; } }

        /// <summary>
        /// root object type
        /// </summary>
        public FsObjType RootType { get { return _RootType; } }

        /// <summary>
        /// how long in minutes to retain items
        /// </summary>
        public float RetainMonitoredObjectsMinutes { get { return _RetainMonitoredObjectsMinutes; } }

        /// <summary>
        /// allow per item aging on resources for this worker
        /// </summary>
        public bool FsMaintenanceAllowPerItemAging { get { return _FsMaintenanceAllowPerItemAging; } }

        /// <summary>
        /// minimum preservation depth
        /// </summary>
        public int FsMaintenancePreservationDepth { get { return _FsMaintenancePreservationDepth; } }

        /// <summary>
        /// never delete directories if true
        /// </summary>
        public bool FsMaintenanceNeverDeleteDirectories { get { return _FsMaintenanceNeverDeleteDirectories; } }

        /// <summary>
        /// reset expired directory if never deleting them - use true to reduce impact of repeatedly processing directories will not delete due to config options
        /// </summary>
        public bool FsMaintenanceResetExpiredDirectoriesIfNeverDelete { get { return _FsMaintenanceResetExpiredDirectoriesIfNeverDelete; } }

        /// <summary>
        /// only perform object deletion if all other fs objects in path subtree are older than the aging threshold
        /// </summary>
        public bool FsMaintenanceOnlyDeleteFolderContentsAsGroup { get { return _FsMaintenanceOnlyDeleteFolderContentsAsGroup; } }

        /// <summary>
        /// instantiate a file system maintenance worker
        /// </summary>
        /// <param name="RootConfigItem">root config item</param>
        /// <param name="fsWatchRoot">file watcher root</param>
        /// <param name="FsMaintenanceRetainMonitoredObjectsMinutes">minutes to keep files before exposing to aging actions</param>
        /// <param name="FsMaintenanceAllowPerItemAging">true to allow per item aging</param>
        /// <param name="FsMaintenancePreservationDepth">preservation depth relative to root - 0 anything under root subject to aging actions</param>
        /// <param name="FsMaintenanceNeverDeleteDirectories">true if should never delete objects of type directory</param>
        /// <param name="FsMaintenanceResetExpiredDirectoriesIfNeverDelete">true if should reset aging timer on expired directories if never delete directories true</param>
        /// <param name="FsMaintenanceOnlyDeleteFolderContentsAsGroup">only delete an item if all other items in the subtree are also aged</param>
        public FsMaintenanceWorker(string RootConfigItem, string fsWatchRoot, long FsMaintenanceRetainMonitoredObjectsMinutes,
            bool FsMaintenanceAllowPerItemAging, int FsMaintenancePreservationDepth, bool FsMaintenanceNeverDeleteDirectories,
            bool FsMaintenanceResetExpiredDirectoriesIfNeverDelete, bool FsMaintenanceOnlyDeleteFolderContentsAsGroup)
        {
            _RetainersListIndex = FileAgeRetainer.retainers.FindIndex(item => String.Compare(item.RootConfigItem, RootConfigItem, 0) == 0);
            _RootConfigItem = RootConfigItem;
            _FsWatchRoot = fsWatchRoot;
            _RootType = GetObjType(_FsWatchRoot);
            _MaintenanceFrequencyMs = Properties.Settings.Default.FsMaintenanceWorkerFrequencyMs;
            _RetainMonitoredObjectsMinutes = FsMaintenanceRetainMonitoredObjectsMinutes;
            _FsMaintenanceAllowPerItemAging = FsMaintenanceAllowPerItemAging;
            _FsMaintenancePreservationDepth = FsMaintenancePreservationDepth;
            _FsRootDepth = _FsWatchRoot.TrimEnd('\\').Split('\\').Length;
            _FsMaintenanceNeverDeleteDirectories = FsMaintenanceNeverDeleteDirectories;
            _FsMaintenanceResetExpiredDirectoriesIfNeverDelete = FsMaintenanceResetExpiredDirectoriesIfNeverDelete;
            _FsMaintenanceOnlyDeleteFolderContentsAsGroup = FsMaintenanceOnlyDeleteFolderContentsAsGroup;
        }

        /// <summary>
        /// perform file system maintenance tasks
        /// </summary>
        /// <param name="state">state metadata, used by event reset</param>
        internal void PerformFsMaintenance(object state)
        {
            AutoResetEvent autoEvent = (AutoResetEvent)state;

            if (FileAgeRetainer.RetainersInitializing)
            {
                DiagnosticsEventHandler.LogEvent(300, "File System Maintenance worker " + _RootConfigItem + " ...cannot work, retainers initializing...", EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
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
                DiagnosticsEventHandler.LogEvent(301, "File System Maintenance worker " + _RootConfigItem + " ...cannot work, cannot find retainer...", EventLogEntryType.Error, DiagnosticsEventHandler.OutputChannels.DebugConsole);
                autoEvent.Set();
                return;
            }

            if (_Filter == null)
            {
                _Filter = FileAgeRetainer.retainers[_RetainersListIndex].Filter;
            }

            if (!Monitor.TryEnter(FileAgeRetainer.retainers[_RetainersListIndex].MainteanceWorkerTimerLock))
            {
                DiagnosticsEventHandler.LogEvent(302, "File System Maintenance worker " + _RootConfigItem + " ...cannot work, already in timer critical section...", EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
                autoEvent.Set();
                return;
            }

#if DEBUG
            DiagnosticsEventHandler.LogEvent(303, "File System Maintenance worker " + _RootConfigItem + " ...woke to do work...", EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
#endif
            Stopwatch workTimer = new Stopwatch();

            workTimer.Start();

            PerformMaintenance();

            Monitor.Exit(FileAgeRetainer.retainers[_RetainersListIndex].MainteanceWorkerTimerLock);

            workTimer.Stop();
#if DEBUG
            DiagnosticsEventHandler.LogEvent(304, "File System Maintenance worker " + _RootConfigItem + " ...done. any more need doing?... " + workTimer.ElapsedMilliseconds + "ms", EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
#endif

            autoEvent.Set();

        }

        /// <summary>
        /// perform file system maintenance
        /// </summary>
        private void PerformMaintenance()
        {
            long nowTicks = DateTime.Now.Ticks;

            DateTime nowTime = new DateTime(nowTicks);

            DateTime boundaryTime = nowTime.Subtract(TimeSpan.FromMinutes(_RetainMonitoredObjectsMinutes));


            if (_FsMaintenanceAllowPerItemAging)
            {
                PerformPerItemAgeBasedDeletions(boundaryTime.Ticks);
            }
        }

        /// <summary>
        /// perform per item aging deletions based on boundary ticks - timespan greater than preservation minutes for config line item (or defaults if not specified)
        /// </summary>
        /// <param name="boundaryTicks"></param>
        private void PerformPerItemAgeBasedDeletions(long boundaryTicks)
        {
            foreach (var item in FileAgeRetainer.retainers[_RetainersListIndex].fsCachedInfoDictionary.Where(kvp => kvp.Value.PreservationTimeStart < boundaryTicks).Select(kvp => kvp.Value))
            {
                if ((item.FullPath.Split('\\').Length - _FsRootDepth) > _FsMaintenancePreservationDepth)
                {

                    switch (item.Type)
                    {
                        case FsObjType.File:

                            if (_FsMaintenanceOnlyDeleteFolderContentsAsGroup)
                            {
                                FileInfo fi = new FileInfo(item.FullPath);
                                //check every other file in the directory is also old enough to delete.  this could change at any instant... so check each time...
                                var max = FileAgeRetainer.retainers[_RetainersListIndex].fsCachedInfoDictionary.Where(kvp => kvp.Value.FullPath.Contains(fi.DirectoryName) && kvp.Value.Type == FsObjType.File).Aggregate((l, r) => l.Value.PreservationTimeStart > r.Value.PreservationTimeStart ? l : r).Value.PreservationTimeStart;
                                if (max < boundaryTicks)
                                {
                                    try
                                    {
                                        File.Delete(item.FullPath);
                                        DiagnosticsEventHandler.LogEvent(305, "File System Maintenance worker " + _RootConfigItem + " deleted file: " + item.FullPath + " - was older than " + _RetainMonitoredObjectsMinutes + "min", EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.ApplicationLog | DiagnosticsEventHandler.OutputChannels.DebugConsole);
                                    }
                                    catch (Exception iox)
                                    {
                                        DiagnosticsEventHandler.LogEvent(306, "ERROR - File System Maintenance worker " + _RootConfigItem + " could not delete aged file: " + item.FullPath + " - " + iox.Message, EventLogEntryType.Error);
                                    }
                                }
#if DEBUG
                                else
                                {
                                    DiagnosticsEventHandler.LogEvent(307, "File System Maintenance worker " + _RootConfigItem + " config prevents file delete: " + item.FullPath + " - newest object in " + fi.DirectoryName + " - " + max, EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.ApplicationLog | DiagnosticsEventHandler.OutputChannels.DebugConsole);
                                }
#endif
                            }
                            else
                            {
                                try
                                {
                                    File.Delete(item.FullPath);
                                    DiagnosticsEventHandler.LogEvent(308, "File System Maintenance worker " + _RootConfigItem + " deleted file: " + item.FullPath + " - was older than " + _RetainMonitoredObjectsMinutes + "min", EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.ApplicationLog | DiagnosticsEventHandler.OutputChannels.DebugConsole);
                                }
                                catch (Exception iox)
                                {
                                    DiagnosticsEventHandler.LogEvent(309, "ERROR - File System Maintenance worker " + _RootConfigItem + " could not delete aged file: " + item.FullPath + " - " + iox.Message, EventLogEntryType.Error);
                                }
                            }
                            break;
                        case FsObjType.Directory:
                            if (_FsMaintenanceNeverDeleteDirectories)
                            {
                                //if true reset the preservation start time on directories which are coming to be expired, to prevent them showing up constantly in the item list to process
                                //quicker runtime if true, but means if switch FsMaintenanceNeverDeleteDirectories to false from true that takes a full aging cycle to process directory deletes from fs
                                if (FsMaintenanceResetExpiredDirectoriesIfNeverDelete)
                                {
                                    item.PreservationTimeStart = DateTime.Now.Ticks;
                                    FileAgeRetainer.retainers[_RetainersListIndex].fsCachedInfoDictionary.AddOrUpdate(
                                    item.FullPath,
                                    item,
                                    (key, oldValue) => item);
#if DEBUG
                                    DiagnosticsEventHandler.LogEvent(310, "File System Maintenance worker " + _RootConfigItem + " reset preservation on: " + item.FullPath + " - due to FsMaintenanceResetExpiredDirectoriesIfNeverDelete:True", EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
#endif
                                }
                            }
                            else
                            {
                                List<string> directoryContents = GetFsItemList(item.FullPath, item.Type, true, "*.*", true, true);
                                if (directoryContents.Count == 0)
                                {
                                    try
                                    {
                                        Directory.Delete(item.FullPath);
                                        DiagnosticsEventHandler.LogEvent(311, "File System Maintenance worker " + _RootConfigItem + " deleted empty directory: " + item.FullPath + " - was older than " + _RetainMonitoredObjectsMinutes + "min", EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.ApplicationLog | DiagnosticsEventHandler.OutputChannels.DebugConsole);
                                    }
                                    catch (Exception dirIoEx)
                                    {
                                        DiagnosticsEventHandler.LogEvent(312, "ERROR - File System Maintenance worker " + _RootConfigItem + " could not delete aged empty directory: " + item.FullPath + " - " + dirIoEx.Message, EventLogEntryType.Error);
                                    }
                                }
                            }
                            //if its not empty do nothing, wait for the contents to age out...
                            break;
                        case FsObjType.NotExist:
                            DiagnosticsEventHandler.LogEvent(313, "File System Maintenance worker " + _RootConfigItem + " asked to delete non-existent file: " + item.FullPath + " - could not act", EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.ApplicationLog | DiagnosticsEventHandler.OutputChannels.DebugConsole);
                            break;
                        default:
                            DiagnosticsEventHandler.LogEvent(314, "ERROR - bad logic in PerformPerItemAgeBasedDeletions(long boundaryTicks): " + _RootConfigItem + " " + item.FullPath, EventLogEntryType.Error);
                            throw new ApplicationException("bad logic in PerformPerItemAgeBasedDeletions(long boundaryTicks): " + _RootConfigItem + " " + item.FullPath);
                    }
                }
            }
        }

    }
}
