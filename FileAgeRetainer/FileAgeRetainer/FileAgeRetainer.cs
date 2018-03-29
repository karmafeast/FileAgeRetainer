using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.IO;
using System.Threading.Tasks.Dataflow;
using System.Threading;

/// <summary>
/// do stuff with files...
/// </summary>
namespace FileAgeRetainer
{
    /// <summary>
    /// FileAgeRetainer service class
    /// </summary>
    public partial class FileAgeRetainer : ServiceBase
    {
        /// <summary>
        /// list of FileAgeRetainerItem - these accessed by retainer item workers directly after they determine thier index position.  allows for multiple retainers to operate safely together when all working in different threads and on different watch roots
        /// </summary>
        public static List<FileAgeRetainerItem> retainers;

        /// <summary>
        /// true if the retainer items for this service are currently initializing, will block queue watcher work
        /// </summary>
        public static bool RetainersInitializing;
        private static System.Timers.Timer startupTimer;

        /// <summary>
        /// instantiate FileAgeRetainer
        /// </summary>
        public FileAgeRetainer()
        {
            InitializeComponent();
        }

        /// <summary>
        /// executed at service startup
        /// </summary>
        /// <param name="args">arguments passed to service used at startup</param>
        protected override void OnStart(string[] args)
        {
            DiagnosticsEventHandler.LogEvent(1, System.Reflection.Assembly.GetExecutingAssembly().FullName + " - under " + System.Reflection.Assembly.GetExecutingAssembly().ImageRuntimeVersion + " - SERVICE STARTING", EventLogEntryType.Information);

            //artificial sleep to give time to attach debugger
            System.Threading.Thread.Sleep(9999);

            retainers = new List<FileAgeRetainerItem>();

            SetInitWorkToTimer();  //do the big work out of a time elapsed event so the service appears to start clean from control commands.
        }

        /// <summary>
        /// prepare time and start it for service initialization work - to prevent service control from being unresponsive due to long running process
        /// </summary>
        private void SetInitWorkToTimer()
        {
            startupTimer = new System.Timers.Timer(5000D);
            startupTimer.AutoReset = false;
            startupTimer.Elapsed += new System.Timers.ElapsedEventHandler(this.startupTimer_Elapsed);
            startupTimer.Start();
        }

        /// <summary>
        /// setup FileAgeRetainer items in a list.  this memory object accessed by index so items end up in the right caches when multiple monitored fs roots are configured
        /// </summary>
        private void SetupRetainerItems()
        {
            RetainersInitializing = true;

            var fileRoots = Properties.Settings.Default.RetainedFileRoots;

            foreach (string s in fileRoots)
            {
                try
                {
                    retainers.Add(new FileAgeRetainerItem(s));
                }
                catch (Exception ex1)
                {
                    DiagnosticsEventHandler.LogEvent(2, "failed init - " + ex1.Message, EventLogEntryType.Error);
                    FileAgeRetainerController.Stop();
                }
            }

            RetainersInitializing = false;
        }

        /// <summary>
        /// when initialization timer elapses, setup the retainer items
        /// </summary>
        /// <param name="sender">event metadata on sender</param>
        /// <param name="e">arguements from the event, not utilized here</param>
        private void startupTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            SetupRetainerItems();
        }

        /// <summary>
        /// shut down the service when a 'stop' command is given
        /// </summary>
        protected override void OnStop()
        {
            DiagnosticsEventHandler.LogEvent(65535, "FileAgeRetainer stop init...", EventLogEntryType.Information);
            for (int i = 0; i < retainers.Count; i++)
            {
                retainers[i].Shutdown();
            }
        }

    }
}
