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
    internal class SerializationWorker
    {
        private string _RootConfigItem;
        private int _RetainersListIndex;
        private int _SerializationFrequencyMs;
        private string _HashForSerialize;

        /// <summary>
        /// root config item for this serialization worker
        /// </summary>
        public string RootConfigItem { get { return _RootConfigItem; } }

        /// <summary>
        /// hash of the file name to be used for the serialization - done in the hashing algorithm associated with this item's retainer, based on the root config item string
        /// </summary>
        public string HashForSerialize { get { return _HashForSerialize; } }

        /// <summary>
        /// instantiate a serialization worker for a root config item
        /// </summary>
        /// <param name="RootConfigItem">root config item string</param>
        public SerializationWorker(string RootConfigItem)
        {
            _RetainersListIndex = FileAgeRetainer.retainers.FindIndex(item => String.Compare(item.RootConfigItem, RootConfigItem, 0) == 0);
            _RootConfigItem = RootConfigItem;
            _SerializationFrequencyMs = Properties.Settings.Default.CacheSerializationFrequencyMs;
            _HashForSerialize = GetHashString(_RootConfigItem, HashAlgorithm);
        }

        /// <summary>
        /// perform dictionary serialization for associated retainer
        /// </summary>
        /// <param name="state">state metadata used for auto reset</param>
        internal void SerializeDictionary(object state)
        {
            AutoResetEvent autoEvent = (AutoResetEvent)state;

            if (FileAgeRetainer.RetainersInitializing)
            {
                DiagnosticsEventHandler.LogEvent(500, "serialization worker " + _RootConfigItem + " ...cannot work, retainers initializing...", EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
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
                DiagnosticsEventHandler.LogEvent(501, "serialization worker " + _RootConfigItem + " ...cannot work, cannot find retainer...", EventLogEntryType.Error, DiagnosticsEventHandler.OutputChannels.DebugConsole | DiagnosticsEventHandler.OutputChannels.Slack);
                autoEvent.Set();
                return;
            }


            //safe to enter critical section
            if (!Monitor.TryEnter(FileAgeRetainer.retainers[_RetainersListIndex].SerializationTimerLock))
            {
                DiagnosticsEventHandler.LogEvent(502, "serialization worker " + _RootConfigItem + " ...cannot work, already in timer critical section...", EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
                autoEvent.Set();
                return;
            }

#if DEBUG
            DiagnosticsEventHandler.LogEvent(503, "serialization worker " + _RootConfigItem + " ...woke to do work...", EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
#endif
            Stopwatch workTimer = new Stopwatch();

            workTimer.Start();

            try
            {
                SerializeDictionary();
            }
            catch (Exception ex)
            {
                workTimer.Stop();
#if DEBUG
                DiagnosticsEventHandler.LogEvent(504, "ERROR - serialization worker " + _RootConfigItem + " - " + ex.Message + " - " + workTimer.ElapsedMilliseconds + "ms", EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole | DiagnosticsEventHandler.OutputChannels.Slack);
#endif

                Monitor.Exit(FileAgeRetainer.retainers[_RetainersListIndex].SerializationTimerLock);
                autoEvent.Set();
                return;
            }

            Monitor.Exit(FileAgeRetainer.retainers[_RetainersListIndex].SerializationTimerLock);

            workTimer.Stop();

#if DEBUG
            DiagnosticsEventHandler.LogEvent(505, "serialization worker " + _RootConfigItem + " ...work complete... " + workTimer.ElapsedMilliseconds + "ms", EventLogEntryType.Information, DiagnosticsEventHandler.OutputChannels.DebugConsole);
#endif

            autoEvent.Set();
        }

        /// <summary>
        /// perform dictionary serialization for the associated retainer item file output working directory, config line item string hash.hashing algorithm
        /// </summary>
        private void SerializeDictionary()
        {
            ConcurrentDictionary<string, CachedFsInfo> dictionaryCopy;
            dictionaryCopy = FileAgeRetainer.retainers[_RetainersListIndex].fsCachedInfoDictionary;
            File.WriteAllText(ExecutingAssemblyDirectory + @"\" + _HashForSerialize + "." + HashAlgorithm, JsonConvert.SerializeObject(dictionaryCopy));
        }
    }
}