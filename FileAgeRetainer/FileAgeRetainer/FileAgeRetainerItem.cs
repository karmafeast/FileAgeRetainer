using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
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
    /// a FileAgeRetainerItem - controls the various work functions and hosts cached dictionary.  FileAgeRetainerItem work independently with their own threads
    /// </summary>
    public class FileAgeRetainerItem
    {
        private string _RootConfigItem;
        private string _FsWatchRoot;
        private FsObjType _RootType;
        private bool _Subtree;
        private string _Filter;
        private long _FsMaintenanceRetainMonitoredObjectsMinutes;
        private bool _FsTypeChangeCausePreservationReset;
        private bool _FsChecksumChangeCausePreservationReset;
        private FsHashAlgorithm _FsHashAlgorithm;
        private bool _FsHashCalculateWholeDirectoryHashes;
        private bool _QueueProcessorIgnoreChangedWhenCreatedThisBatch;
        private bool _FsRenameCausePreservationReset;
        private double _CacheSerializationCacheTooOldDays;
        private bool _FsMaintenanceAttemptOportunisticCompression;
        private bool _FsMaintenanceAllowPerItemAging;
        private int _FsMaintenancePreservationDepth;
        private bool _FsMaintenanceNeverDeleteDirectories;
        private bool _FsMaintenanceResetExpiredDirectoriesIfNeverDelete;
        private bool _FsMaintenanceOnlyDeleteFolderContentsAsGroup;

        private System.IO.FileSystemWatcher _fsWatcher;

        internal ConcurrentQueue<FileSystemEventArgs> _fsCreatedItemQueue;
        internal ConcurrentQueue<FileSystemEventArgs> _fsChangedItemQueue;
        internal ConcurrentQueue<FileSystemEventArgs> _fsDeletedItemQueue;
        internal ConcurrentQueue<RenamedEventArgs> _fsRenamedItemQueue;

        /// <summary>
        /// AutoResetEvent used by this retainer items queue watcher 
        /// </summary>
        private AutoResetEvent AutoEventInstance { get; set; }

        /// <summary>
        /// queue watcher instance for this retainer item
        /// </summary>
        private QueueWatcher QueueWatcherInstance { get; set; }

        /// <summary>
        /// queue watch timer for this retainer item
        /// </summary>
        internal Timer QueueWatchTimer { get; set; }

        /// <summary>
        /// object used for critical section locking by queue watcher
        /// </summary>
        internal object QueueWatchTimerLock = new object();

        /// <summary>
        /// AutoResetEvent used by this retainer items serialization worker
        /// </summary>
        private AutoResetEvent SerializationAutoEventInstance { get; set; }

        /// <summary>
        /// serialization worker instance, flush in memory dictionary for this retainer item to disk periodically to allow for service to restart / stop safely and resume without cache data loss
        /// </summary>
        private SerializationWorker SerializationInstance { get; set; }

        /// <summary>
        /// timer for operation of serialization worker
        /// </summary>
        internal Timer SerializationTimer { get; set; }

        /// <summary>
        /// object used for locking critical section by serialization worker threads
        /// </summary>
        internal object SerializationTimerLock = new object();

        /// <summary>
        /// AutoResetEvent used by file system maintenance worker
        /// </summary>
        private AutoResetEvent MaintenanceAutoEventInstance { get; set; }

        /// <summary>
        /// file system maintenance worker instance for this retainer item
        /// </summary>
        private FsMaintenanceWorker MaintenanceWorkerInstance { get; set; }

        /// <summary>
        /// timer used by file system maintenance wortker to control its frequency of action
        /// </summary>
        internal Timer MaintenanceWorkerTimer { get; set; }

        /// <summary>
        /// object used for lock signalling entry/exit in/out of critical section of maintenance worker
        /// </summary>
        internal object MainteanceWorkerTimerLock = new object();

        internal ConcurrentDictionary<string, CachedFsInfo> fsCachedInfoDictionary;


        /// <summary>
        /// how long, in minutes, to retain an object before exposing it to aging action (e.g. deletion)
        /// </summary>
        public long FsMaintenanceRetainMonitoredObjectsMinutes { get { return _FsMaintenanceRetainMonitoredObjectsMinutes; } }

        /// <summary>
        /// true if file system change causes preservation time reset
        /// </summary>
        public bool FsTypeChangeCausePreservationReset { get { return _FsTypeChangeCausePreservationReset; } }

        /// <summary>
        /// true if file checksum changes cause preservation time reset
        /// </summary>
        public bool FsChecksumChangeCausePreservationReset { get { return _FsChecksumChangeCausePreservationReset; } }

        /// <summary>
        /// hashing algorithm used
        /// </summary>
        public FsHashAlgorithm FsHashAlgorithm { get { return _FsHashAlgorithm; } }

        /// <summary>
        /// true if should has all directory contents - EXPENSIVE
        /// </summary>
        public bool FsHashCalculateWholeDirectoryHashes { get { return _FsHashCalculateWholeDirectoryHashes; } }

        /// <summary>
        /// true if change events for a file should be ignored when the item was created in the same timer block as the change occured - supresses processing multiple change events which occur as atomic operations on things like a file create
        /// </summary>
        public bool QueueProcessorIgnoreChangedWhenCreatedThisBatch { get { return _QueueProcessorIgnoreChangedWhenCreatedThisBatch; } }

        /// <summary>
        /// true if file system object rename causes preservation time reset
        /// </summary>
        public bool FsRenameCausePreservationReset { get { return _FsRenameCausePreservationReset; } }

        /// <summary>
        /// the number of days over which a serialized cache is considered 'too old' and is ignored - dictionary rebuilt in this case, and all items considered 'new' 
        /// </summary>
        public double CacheSerializationCacheTooOldDays { get { return _CacheSerializationCacheTooOldDays; } }

        /// <summary>
        /// true if file system mainteance should include opportunistic compression (NOT YET IMPLEMENTED)
        /// </summary>
        public bool FsMaintenanceAttemptOportunisticCompression { get { return _FsMaintenanceAttemptOportunisticCompression; } }

        /// <summary>
        /// true if mainteance worker should perform per item aging actions - this will result in deletions happening in sweeps, from furthest down the file system tree to nearest root as directories age out and are deleted - respects options for never deleting directories, minimum preservation depth etc via other settings
        /// </summary>
        public bool FsMaintenanceAllowPerItemAging { get { return _FsMaintenanceAllowPerItemAging; } }

        /// <summary>
        /// the depth of file system objects files/directories which should be preserved regardless of their age - 0, anything under root OK. e.g. 1 on c:\temp\... would preserve c:\temp\preserveme directory and c:\temp\dontdeleteme.txt but NOT c:\temp\preserveme\killthis or c:\temp\preserveme\somefile.txt
        /// </summary>
        public int FsMaintenancePreservationDepth { get { return _FsMaintenancePreservationDepth; } }

        /// <summary>
        /// never delete directories, only ever files.  will leave empty directories in file system as files age out.  use with FsMaintenanceResetExpiredDirectoriesIfNeverDelete to stop directories legitimately thought to be aged from reappearing in processing results for this mainteance worker
        /// </summary>
        public bool FsMaintenanceNeverDeleteDirectories { get { return _FsMaintenanceNeverDeleteDirectories; } }

        /// <summary>
        /// if never delete directories, set to true to reset preservation start time to now, will prevent maintenance worker from having to process and ignore all directory items considered aged legitimately. optimization, typically set true
        /// </summary>
        public bool FsMaintenanceResetExpiredDirectoriesIfNeverDelete { get { return _FsMaintenanceResetExpiredDirectoriesIfNeverDelete; } }

        /// <summary>
        /// only delete a file if all other files in the directory in which it exist are ALSO aged
        /// </summary>
        public bool FsMaintenanceOnlyDeleteFolderContentsAsGroup { get { return _FsMaintenanceOnlyDeleteFolderContentsAsGroup; } }

        /// <summary>
        /// the root config item that this Maintenacne worker is based on
        /// </summary>
        public string RootConfigItem { get { return _RootConfigItem; } }

        /// <summary>
        /// FSObjType of the root of this mainteance worker - file/directory/notExist
        /// </summary>
        public FsObjType RootType { get { return _RootType; } }


        /// <summary>
        /// true if the subtree of the root is being maintained
        /// </summary>
        public bool Subtree { get { return _Subtree; } set { _Subtree = value; } }

        /// <summary>
        /// clean root path as appropriate for FileSystemWatcher instantiation for this maintenance worker
        /// </summary>
        public string FsWatchRoot { get { return _FsWatchRoot; } }


        /// <summary>
        /// the filter used for this maintenance worker's filesystemwatcher - NOT YET IMPLEMENTED, assumed *.*
        /// </summary>
        public string Filter { get { return _Filter; } }

        /// <summary>
        /// instantiate a FileAgeRetainerItem for the specified rootItemString - setup dictionary, configure and start a FileSystemWatcher, setup and start a 'queue watcher' worker, setup and start a 'serialization worker', setup and start a FsMaintenanceWorker
        /// </summary>
        /// <param name="rootItemString">the config item string for the retainer item e.g. "c:\temp\..."</param>
        public FileAgeRetainerItem(string rootItemString)
        {
            //parse the config items differently
            string[] configItemSplit = rootItemString.Split('|');
            _Subtree = false;
            _Filter = "*.*";  //TODO: allow filter setting

            //set defaults from config... 
            SetGlobalApplicationDefaults();

            //...then override for config line item
            SetPerConfigItemOverrides(rootItemString);


            _RootConfigItem = configItemSplit[0];

            _RootType = GetRootType(configItemSplit[0]);

            string root = null;

            try
            { root = GetCleanRootandSetObjType(configItemSplit[0]); }
            catch (Exception rootEx)
            {
                DiagnosticsEventHandler.LogEvent(200, "ERROR - retainer item setup - root config item " + _RootConfigItem + " - " + rootEx.Message, EventLogEntryType.Error);
                throw new ApplicationException("retainer item setup for " + _RootConfigItem + " failed... check config");
            }

            _Subtree = GetSubtreeForConfigItem(configItemSplit[0]);

            _FsWatchRoot = root;

            AutoEventInstance = new AutoResetEvent(false);
            QueueWatcherInstance = new QueueWatcher(_RootConfigItem, _QueueProcessorIgnoreChangedWhenCreatedThisBatch, _FsTypeChangeCausePreservationReset, _FsChecksumChangeCausePreservationReset, _FsRenameCausePreservationReset, _FsHashAlgorithm, _FsHashCalculateWholeDirectoryHashes);

            _fsCreatedItemQueue = new ConcurrentQueue<FileSystemEventArgs>();
            _fsChangedItemQueue = new ConcurrentQueue<FileSystemEventArgs>();
            _fsDeletedItemQueue = new ConcurrentQueue<FileSystemEventArgs>();
            _fsRenamedItemQueue = new ConcurrentQueue<RenamedEventArgs>();

            //queue watch timer delegate
            TimerCallback timerDelegate = new TimerCallback(QueueWatcherInstance.ProcessQueues);


            DiagnosticsEventHandler.LogEvent(201, "setting up fs queue watcher for " + root + " - " + Properties.Settings.Default.QueueProcesserTimerIntervalMs + "ms", EventLogEntryType.Information);

            Stopwatch initTimer = new Stopwatch();

            DiagnosticsEventHandler.LogEvent(202, "FileAgeRetainerItem init start for " + _RootConfigItem, EventLogEntryType.Information);
            initTimer.Start();

            InitializeFsCachedInfoDictionary();
            SetupFileSystemWatcher(root); //setup this._fsWatcher from clean root string

            initTimer.Stop();

            DiagnosticsEventHandler.LogEvent(203, "FileAgeRetainerItem init complete for " + _RootConfigItem + " item count: " + fsCachedInfoDictionary.Count + " init time: " + initTimer.ElapsedMilliseconds + "ms", EventLogEntryType.Information);

            //wait 10s then start the queue watch timer
            QueueWatchTimer = new Timer(timerDelegate, AutoEventInstance, 10000, Properties.Settings.Default.QueueProcesserTimerIntervalMs);

            SetupSerializationWorker();

            SetupMaintenanceWorker();

        }

        /// <summary>
        /// parse per config item options from lines in app.config RetainedFileRoots - override global defaults for this config line item
        /// </summary>
        /// <param name="rootItemString">the config line item string to parse</param>
        private void SetPerConfigItemOverrides(string rootItemString)
        {
            string[] configItemSplit = rootItemString.Split('|');

            if (configItemSplit.Length > 1)
            {
                string[] propertiesInConfigLine = new string[configItemSplit.Length - 1];

                //put everything but the first item in another array
                for (int i = 1; i < configItemSplit.Length; i++)
                {
                    propertiesInConfigLine[i - 1] = configItemSplit[i];
                }

                Parallel.ForEach(propertiesInConfigLine, CommonComponents.ParallelOptions, item =>
                {
                    string[] propSplit = item.Split(':');

                    foreach (SettingsProperty sp in Properties.Settings.Default.Properties)
                    {
                        try
                        {

                            if (sp.Name == propSplit[0])
                            {
                                switch (propSplit[0])
                                {
                                    case "FsMaintenanceRetainMonitoredObjectsMinutes":
                                        {
                                            _FsMaintenanceRetainMonitoredObjectsMinutes = Convert.ToInt64(propSplit[1]);
                                        }
                                        break;
                                    case "FsTypeChangeCausePreservationReset":
                                        {
                                            _FsTypeChangeCausePreservationReset = Boolean.Parse(propSplit[1]);
                                        }
                                        break;
                                    case "FsChecksumChangeCausePreservationReset":
                                        {
                                            _FsChecksumChangeCausePreservationReset = Boolean.Parse(propSplit[1]);
                                        }
                                        break;
                                    case "FsHashAlgorithm":
                                        {
                                            bool configHashAlgOk = Enum.TryParse<FsHashAlgorithm>(propSplit[1], out _FsHashAlgorithm);
                                            if (!configHashAlgOk) { DiagnosticsEventHandler.LogEvent(204, "could not parse desired hash algorithm from app.config: " + Properties.Settings.Default.FsHashAlgorithm, EventLogEntryType.Warning); _FsHashAlgorithm = FsHashAlgorithm.xxHash; }
                                        }
                                        break;
                                    case "FsHashCalculateWholeDirectoryHashes":
                                        {
                                            _FsHashCalculateWholeDirectoryHashes = Boolean.Parse(propSplit[1]);
                                        }
                                        break;
                                    case "QueueProcessorIgnoreChangedWhenCreatedThisBatch":
                                        {
                                            _QueueProcessorIgnoreChangedWhenCreatedThisBatch = Boolean.Parse(propSplit[1]);
                                        }
                                        break;
                                    case "FsRenameCausePreservationReset":
                                        {
                                            _FsRenameCausePreservationReset = Boolean.Parse(propSplit[1]);
                                        }
                                        break;
                                    case "CacheSerializationCacheTooOldDays":
                                        {
                                            _CacheSerializationCacheTooOldDays = Double.Parse(propSplit[1]);
                                        }
                                        break;
                                    case "FsMaintenanceAttemptOportunisticCompression":
                                        {
                                            _FsMaintenanceAttemptOportunisticCompression = Boolean.Parse(propSplit[1]);
                                        }
                                        break;
                                    case "FsMaintenanceAllowPerItemAging":
                                        {
                                            _FsMaintenanceAllowPerItemAging = Boolean.Parse(propSplit[1]);
                                        }
                                        break;
                                    case "FsMaintenancePreservationDepth":
                                        {
                                            _FsMaintenancePreservationDepth = Int32.Parse(propSplit[1]);
                                        }
                                        break;
                                    case "FsMaintenanceNeverDeleteDirectories":
                                        {
                                            _FsMaintenanceNeverDeleteDirectories = Boolean.Parse(propSplit[1]);
                                        }
                                        break;
                                    case "FsMaintenanceResetExpiredDirectoriesIfNeverDelete":
                                        {
                                            _FsMaintenanceResetExpiredDirectoriesIfNeverDelete = Boolean.Parse(propSplit[1]);
                                        }
                                        break;
                                    case "FsMaintenanceOnlyDeleteFolderContentsAsGroup":
                                        {
                                            _FsMaintenanceOnlyDeleteFolderContentsAsGroup = Boolean.Parse(propSplit[1]);
                                        }
                                        break;
                                    default:
                                        DiagnosticsEventHandler.LogEvent(205, "FileAgeRetainerItem " + _RootConfigItem + " unknown config option: " + item, EventLogEntryType.Error);
                                        throw new ArgumentException("FileAgeRetainerItem " + _RootConfigItem + " could not parse config option: " + item);
                                }


                            }
                        }
                        catch (Exception parseEx)
                        {
                            DiagnosticsEventHandler.LogEvent(206, "FileAgeRetainerItem " + _RootConfigItem + " could not parse config option: " + item + " - " + parseEx.Message, EventLogEntryType.Error);
                            continue;
                        }
                    }

                });
            }
        }

        /// <summary>
        /// configure global options from app.config for this FileAgeRetainerItem - any overrides to this default config should be processed after this default init
        /// </summary>
        private void SetGlobalApplicationDefaults()
        {
            _FsMaintenanceRetainMonitoredObjectsMinutes = Properties.Settings.Default.FsMaintenanceRetainMonitoredObjectsMinutes;
            _FsTypeChangeCausePreservationReset = Properties.Settings.Default.FsTypeChangeCausePreservationReset;
            _FsChecksumChangeCausePreservationReset = Properties.Settings.Default.FsChecksumChangeCausePreservationReset;
            bool configHashAlgOk = Enum.TryParse<FsHashAlgorithm>(Properties.Settings.Default.FsHashAlgorithm, out _FsHashAlgorithm);
            if (!configHashAlgOk) { DiagnosticsEventHandler.LogEvent(207, "could not parse desired hash algorithm from app.config: " + Properties.Settings.Default.FsHashAlgorithm, EventLogEntryType.Warning); _FsHashAlgorithm = FsHashAlgorithm.xxHash; }
            _FsHashCalculateWholeDirectoryHashes = Properties.Settings.Default.FsHashCalculateWholeDirectoryHashes;
            _QueueProcessorIgnoreChangedWhenCreatedThisBatch = Properties.Settings.Default.QueueProcessorIgnoreChangedWhenCreatedThisBatch;
            _FsRenameCausePreservationReset = Properties.Settings.Default.FsRenameCausePreservationReset;
            _CacheSerializationCacheTooOldDays = Properties.Settings.Default.CacheSerializationCacheTooOldDays;
            _FsMaintenanceAttemptOportunisticCompression = Properties.Settings.Default.FsMaintenanceAttemptOportunisticCompression;
            _FsMaintenanceAllowPerItemAging = Properties.Settings.Default.FsMaintenanceAllowPerItemAging;
            _FsMaintenanceNeverDeleteDirectories = Properties.Settings.Default.FsMaintenanceNeverDeleteDirectories;
            _FsMaintenanceResetExpiredDirectoriesIfNeverDelete = Properties.Settings.Default.FsMaintenanceResetExpiredDirectoriesIfNeverDelete;

            _FsMaintenanceOnlyDeleteFolderContentsAsGroup = Properties.Settings.Default.FsMaintenanceOnlyDeleteFolderContentsAsGroup;
        }

        /// <summary>
        /// get root object type from config line item
        /// </summary>
        /// <param name="v">the confile line item to check</param>
        /// <returns>the object type of the root</returns>
        private FsObjType GetRootType(string v)
        {
            string root = TrimRootOptionsSuffix(v);

            return GetObjType(root);
        }

        /// <summary>
        /// determine if config item string sets 'subtree' as indicated by trailing ... - e.g. c:\temp\...
        /// </summary>
        /// <param name="v">config string</param>
        /// <returns>true if subtree should be monitored by the retainer FileSystemWatcher</returns>
        private bool GetSubtreeForConfigItem(string v)
        {
            if (v.EndsWith("\\..."))
            {
                return true;
            }
            else { return false; }
        }

        /// <summary>
        /// trim the config line item - for the purposes of obtaining a clean file root from the config line item
        /// </summary>
        /// <param name="rootConfigWatchString">config line item</param>
        /// <returns>trimmed string</returns>
        private string TrimRootOptionsSuffix(string rootConfigWatchString)
        {
            string root = rootConfigWatchString;
            if (rootConfigWatchString.EndsWith("\\..."))
            {
                root = rootConfigWatchString.Substring(0, rootConfigWatchString.Length - 4);
            }

            return root;
        }

        /// <summary>
        /// obtain a clean file path of the config line item root
        /// </summary>
        /// <param name="v">config line item</param>
        /// <returns>clean root string and _RootType set as appropriate to the root in this object</returns>
        private string GetCleanRootandSetObjType(string v)
        {
            string root = TrimRootOptionsSuffix(v);

            switch (_RootType)
            {
                case FsObjType.File:
                    break;
                case FsObjType.Directory:
                    if (!root.EndsWith("\\"))
                    {
                        root += "\\";
                    }
                    break;
                case FsObjType.NotExist:
                    throw new ApplicationException("Non-existent object type for retainer: " + root);
                default:
                    throw new ApplicationException("bad logic in retainer constructor: " + root);
            }

            return root;
        }

        /// <summary>
        /// configure and start a FS Maintenance worker for this retainer item
        /// </summary>
        private void SetupMaintenanceWorker()
        {
            DiagnosticsEventHandler.LogEvent(208, "setting up file system mainteance worker for " + _RootConfigItem + " - " + Properties.Settings.Default.FsMaintenanceWorkerFrequencyMs + "ms", EventLogEntryType.Information);
            MaintenanceAutoEventInstance = new AutoResetEvent(false);
            MaintenanceWorkerInstance = new FsMaintenanceWorker(_RootConfigItem, _FsWatchRoot, _FsMaintenanceRetainMonitoredObjectsMinutes, _FsMaintenanceAllowPerItemAging, _FsMaintenancePreservationDepth, _FsMaintenanceNeverDeleteDirectories, _FsMaintenanceResetExpiredDirectoriesIfNeverDelete, _FsMaintenanceOnlyDeleteFolderContentsAsGroup);
            //delegate for serialization worker
            TimerCallback maintenanceWorkerDelegate = new TimerCallback(MaintenanceWorkerInstance.PerformFsMaintenance);
            MaintenanceWorkerTimer = new Timer(maintenanceWorkerDelegate, MaintenanceAutoEventInstance, Properties.Settings.Default.FsMaintenanceWorkerInitialDelayMs, Properties.Settings.Default.FsMaintenanceWorkerFrequencyMs);
        }

        /// <summary>
        /// configure and start a serialization worker for this retainer item
        /// </summary>
        private void SetupSerializationWorker()
        {
            DiagnosticsEventHandler.LogEvent(209, "setting up dictionary serialization worker for " + _RootConfigItem + " - " + Properties.Settings.Default.CacheSerializationFrequencyMs + "ms", EventLogEntryType.Information);
            SerializationAutoEventInstance = new AutoResetEvent(false);
            SerializationInstance = new SerializationWorker(_RootConfigItem);
            //delegate for serialization worker
            TimerCallback serializationDelegate = new TimerCallback(SerializationInstance.SerializeDictionary);
            SerializationTimer = new Timer(serializationDelegate, SerializationAutoEventInstance, Properties.Settings.Default.CacheSerializationFrequencyMs, Properties.Settings.Default.CacheSerializationFrequencyMs);
        }

        /// <summary>
        /// things to do when a retainer item is to be shutdown, typically as service stop
        /// </summary>
        internal void Shutdown()
        {
            this.StopFsWatcher();
            this.QueueWatchTimer.Dispose();
            this.SerializationTimer.Dispose();

            //removed
            //#if DEBUG
            //            DiagnosticsEventHandler.LogEvent(210, "DIAGNOSTIC - DICTIONARY DUMP - " + this._RootConfigItem, EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
            //            foreach (KeyValuePair<string, CachedFsInfo> entry in fsCachedInfoDictionary)
            //            {
            //                DiagnosticsEventHandler.LogEvent(211, entry.Key + " - " + entry.Value.Type + " - " + "preservation time start ticks: " + entry.Value.PreservationTimeStart + " - " + CommonComponents.HashAlgorithm + ": " + entry.Value.HashString, EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
            //            }
            //#endif
        }

        /// <summary>
        /// initialize the caches fs info dictionary for this retainer item
        /// </summary>
        private void InitializeFsCachedInfoDictionary()
        {

            string potentialDictionary = ExecutingAssemblyDirectory + @"\" + GetHashString(_RootConfigItem, HashAlgorithm) + "." + HashAlgorithm;
            bool serializedDictionaryValid = false;

            if (File.Exists(potentialDictionary))
            {
                FileInfo potentialDictionaryInfo = new FileInfo(potentialDictionary);

                TimeSpan howOld = DateTime.Now.Subtract(potentialDictionaryInfo.LastWriteTime);

                if (howOld.TotalDays > _CacheSerializationCacheTooOldDays)
                {
                    DiagnosticsEventHandler.LogEvent(212, "DIAGNOSTIC - found cache - " + _RootConfigItem + " ...TOO OLD... MUST REBUILD... " + potentialDictionary, EventLogEntryType.Warning);
                }
                else
                {
                    serializedDictionaryValid = true;
                }
            }

            if (serializedDictionaryValid)
            {
                DiagnosticsEventHandler.LogEvent(213, "DIAGNOSTIC - found serialized dictionary for - " + _RootConfigItem + " ...deserializing " + potentialDictionary, EventLogEntryType.Information);

                fsCachedInfoDictionary = JsonConvert.DeserializeObject<ConcurrentDictionary<string, CachedFsInfo>>(File.ReadAllText(potentialDictionary));

                DiagnosticsEventHandler.LogEvent(214, "DIAGNOSTIC - deserialized for - " + _RootConfigItem + " - " + fsCachedInfoDictionary.Count + " entries.", EventLogEntryType.Information);

                VerifyDictionary();
                //verify dictionary...

                return;
            }

            BuildDictionary();
        }

        /// <summary>
        /// build the dictionary for this retainer item assuming no knowledge
        /// </summary>
        private void BuildDictionary()
        {
            fsCachedInfoDictionary = new ConcurrentDictionary<string, CachedFsInfo>();

            List<string> files = GetFsItemList(_FsWatchRoot, _RootType, _Subtree, _Filter, true, true);

            Parallel.ForEach(files, CommonComponents.ParallelOptions, item =>
            {
                fsCachedInfoDictionary.TryAdd(item, new CachedFsInfo(item, _FsTypeChangeCausePreservationReset, _FsChecksumChangeCausePreservationReset, _FsRenameCausePreservationReset, _FsHashAlgorithm, _FsHashCalculateWholeDirectoryHashes));
            });
        }


        /// <summary>
        /// verify a deserialized dictionary
        /// </summary>
        private void VerifyDictionary()
        {
            switch (_RootType)
            {
                case FsObjType.File:
                    if (fsCachedInfoDictionary.Count > 1)
                    {
                        //root is a file and dictionary from deserialize has more than one entry... it is invalid
                        DiagnosticsEventHandler.LogEvent(215, "WARNING - file root serialized dictionary load contains more than one entry - must rebuild: " + _FsWatchRoot, EventLogEntryType.Warning);
                        BuildDictionary();
                        return;
                    }
                    if (fsCachedInfoDictionary[_FsWatchRoot].FullPath != _FsWatchRoot)
                    {
                        DiagnosticsEventHandler.LogEvent(216, "WARNING - file root serialized load different single file: " + _FsWatchRoot + " vs serialized: " + fsCachedInfoDictionary[_FsWatchRoot].FullPath + " - must rebuild", EventLogEntryType.Warning);
                        BuildDictionary();
                        return;
                    }
                    return;
                case FsObjType.Directory:

#if DEBUG
                    DiagnosticsEventHandler.LogEvent(217, "Verification of deserialized dictionary - getting files present now for... " + _FsWatchRoot, EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
#endif

                    List<string> files = GetFsItemList(_FsWatchRoot, _RootType, _Subtree, _Filter, true, true);

#if DEBUG
                    DiagnosticsEventHandler.LogEvent(218, "Verification of deserialized dictionary - file and directory item total... " + files.Count, EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
#endif

#if DEBUG
                    DiagnosticsEventHandler.LogEvent(219, "Verification of deserialized dictionary - initial deserialized dictionary count... " + fsCachedInfoDictionary.Count, EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
#endif                

                    Parallel.ForEach(fsCachedInfoDictionary, CommonComponents.ParallelOptions, item =>
                    {
                        CachedFsInfo itemValue = null;
                        if (GetObjType(item.Key) == FsObjType.NotExist)
                        {
                            fsCachedInfoDictionary.TryRemove(item.Key, out itemValue);
                        }
                    });
#if DEBUG
                    DiagnosticsEventHandler.LogEvent(220, "Verification of deserialized dictionary - dictionary count after trim excess... " + fsCachedInfoDictionary.Count, EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
#endif
                    //check all files in dictionary
                    Parallel.ForEach(files, CommonComponents.ParallelOptions, item =>
                    {
                        if (!fsCachedInfoDictionary.ContainsKey(item))
                        {

                            CachedFsInfo newItem = new CachedFsInfo(item, _FsTypeChangeCausePreservationReset, _FsChecksumChangeCausePreservationReset, _FsRenameCausePreservationReset, _FsHashAlgorithm, _FsHashCalculateWholeDirectoryHashes);

                            fsCachedInfoDictionary.AddOrUpdate(
                              newItem.FullPath,
                              newItem,
                              (key, oldValue) => newItem);

#if DEBUG
                            DiagnosticsEventHandler.LogEvent(221, "file: " + newItem.FullPath + " added to cache. missing from cache on deserialize, considered new. checksum: " + newItem.HashString, EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
#endif
                        }
                    });

#if DEBUG
                    DiagnosticsEventHandler.LogEvent(222, "Verification of deserialized dictionary - dictionary post addition missing records count... " + fsCachedInfoDictionary.Count, EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
#endif

                    if (files.Count != fsCachedInfoDictionary.Count)
                    {
                        DiagnosticsEventHandler.LogEvent(223, "ERROR - could not reconcile current fs content versus deserialized dictionary: " + _FsWatchRoot + " rebuilding, all considered new!", EventLogEntryType.Error);
                        BuildDictionary();
                        return;
                    }

                    break;
                case FsObjType.NotExist:
                    DiagnosticsEventHandler.LogEvent(224, "ERROR - Attempt initialize cache dictionary on non existent target: " + _FsWatchRoot, EventLogEntryType.Error);
                    throw new ApplicationException("Attempt initialize cache dictionary on non existent target: " + _FsWatchRoot);
                default:
                    DiagnosticsEventHandler.LogEvent(225, "ERROR - bad logic in initialize cache dictionary: " + _FsWatchRoot, EventLogEntryType.Error);
                    throw new ApplicationException("bad logic in initialize cache dictionary: " + _FsWatchRoot);
            }

            DiagnosticsEventHandler.LogEvent(226, "Validation of dictionary deserialize passed " + fsCachedInfoDictionary.Count + " items", EventLogEntryType.Information);
        }

        /// <summary>
        /// setup and start a FileSystemWatcher for this retainer item root
        /// </summary>
        /// <param name="cleanRoot">clean root as appropriate for FileSystemWatcher instantiation</param>
        private void SetupFileSystemWatcher(string cleanRoot)
        {
            _fsWatcher = new System.IO.FileSystemWatcher();
            _fsWatcher.InternalBufferSize = Properties.Settings.Default.FsWatchInternalBufferBytes; //max out the internal buffer for this thing
            _fsWatcher.IncludeSubdirectories = _Subtree;
            _fsWatcher.Path = cleanRoot;
            _fsWatcher.Filter = _Filter;
            _fsWatcher.NotifyFilter = NotifyFilters.LastAccess | NotifyFilters.LastWrite | NotifyFilters.FileName | NotifyFilters.DirectoryName;

            _fsWatcher.Changed += new FileSystemEventHandler(OnChanged);
            _fsWatcher.Created += new FileSystemEventHandler(OnCreated);
            _fsWatcher.Deleted += new FileSystemEventHandler(OnDeleted);
            _fsWatcher.Renamed += new RenamedEventHandler(OnRenamed);
            _fsWatcher.Error += new ErrorEventHandler(OnError);
            _fsWatcher.EnableRaisingEvents = true;
        }

        /// <summary>
        /// enqueue a changed event, to be processed by QueueWatcher instance for this retainer
        /// </summary>
        /// <param name="sender">sender metadata, not used</param>
        /// <param name="e">the FileSystemEventArgs directly enqueued for speed, QueueWatcher will process this</param>
        private void OnChanged(object sender, FileSystemEventArgs e)
        {
            _fsChangedItemQueue.Enqueue(e);
        }

        /// <summary>
        /// enqueue a created event, to be processed by QueueWatcher instance for this retainer
        /// </summary>
        /// <param name="sender">sender metadata, not used</param>
        /// <param name="e">the FileSystemEventArgs directly enqueued for speed, QueueWatcher will process this</param>
        private void OnCreated(object sender, FileSystemEventArgs e)
        {
            _fsCreatedItemQueue.Enqueue(e);
        }

        /// <summary>
        /// enqueue a deleted event, to be processed by QueueWatcher instance for this retainer
        /// </summary>
        /// <param name="sender">sender metadata, not used</param>
        /// <param name="e">the FileSystemEventArgs directly enqueued for speed, QueueWatcher will process this</param>
        private void OnDeleted(object sender, FileSystemEventArgs e)
        {
            _fsDeletedItemQueue.Enqueue(e);
        }

        /// <summary>
        /// enqueue a renamed event, to be processed by QueueWatcher instance for this retainer
        /// </summary>
        /// <param name="sender">sender metadata, not used</param>
        /// <param name="e">the RenamedEventArgs directly enqueued for speed, QueueWatcher will process this</param>
        private void OnRenamed(object sender, RenamedEventArgs e)
        {
            _fsRenamedItemQueue.Enqueue(e);
        }

        /// <summary>
        /// fires on FileSystemWatcher error for this retainer item - e.g. buffer overruns from extreme file system event rate
        /// </summary>
        /// <param name="source">sender metadata, not used</param>
        /// <param name="e">the event data, including any exception generated</param>
        private static void OnError(object source, ErrorEventArgs e)
        {
            DiagnosticsEventHandler.LogEvent(227, "FileSytemWatcher error...", EventLogEntryType.Error);
            // due to an internal buffer overflow?
            if (e.GetException().GetType() == typeof(InternalBufferOverflowException))
            {
                //  This can happen if Windows is reporting many file system events quickly 
                //  and internal buffer of the  FileSystemWatcher is not large enough to handle this
                //  rate of events. The InternalBufferOverflowException error informs the application
                //  that some of the file system events are being lost.
                DiagnosticsEventHandler.LogEvent(228, "FileSystemWatcher experienced an internal buffer overflow: " + e.GetException().Message, EventLogEntryType.Error);
            }
        }

        /// <summary>
        /// execute when shutting down the FileSystemWatcher for this retainer item
        /// </summary>
        public void StopFsWatcher()
        {
            DiagnosticsEventHandler.LogEvent(229, "FileAgeRetainer " + _RootConfigItem + " stop init...", EventLogEntryType.Information);
            QueueWatchTimer.Dispose();
            _fsWatcher.Dispose();
        }

    }

}
