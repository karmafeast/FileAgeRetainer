using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Configuration;

namespace FileAgeRetainer
{
    /// <summary>
    /// Handler for event logging to various output channels - writes to application log when used in such a fashion to the log with app.config option DiagnosticsEventLog / DiagnosticsEventsource as source / log
    /// </summary>
    static class DiagnosticsEventHandler
    {
        private static EventLog eventLog = new EventLog(Properties.Settings.Default.DiagnosticsEventLog);
        private static string eventSource = Properties.Settings.Default.DiagnosticsEventsource;
        private static string slackWebHook = Properties.Settings.Default.DiagnosticsEventHandlerSlackWebHook;
        private static SlackClient slackClient = null;

        /// <summary>
        /// instantiation of the DiagnosticsEventHandler static class
        /// </summary>
        static DiagnosticsEventHandler()
        {
            CheckRegisterEventLogSource(eventSource, eventLog);
            if (slackWebHook != null && slackWebHook.Length > 0)
            {
                slackClient = new SlackClient(slackWebHook);
            }
        }

        /// <summary>
        /// check for, and register if missing, the event source for the application as specified
        /// </summary>
        /// <param name="eventSource">Event source to use</param>
        /// <param name="eventLog">the log to use</param>
        private static void CheckRegisterEventLogSource(string eventSource, EventLog eventLog)
        {
            if (!EventLog.SourceExists(eventSource))
            {
                EventLog.CreateEventSource(eventSource, eventLog.ToString());

                bool eventSourceOk = false;

#if DEBUG
                Debug.WriteLine("00000 - Creating event source... " + eventSource);
#endif

                while (!eventSourceOk)
                {
                    eventSourceOk = AttemptCreateSampleEvent(eventSource);
                    System.Threading.Thread.Sleep(1000);
#if DEBUG
                    Debug.WriteLine("00001 - INFORMATION - Sleeping 1s for event log creation latency");
#endif
                }
            }

            eventLog.Source = eventSource;
        }

        /// <summary>
        /// create a sample event - used in initialization of eventlog source
        /// </summary>
        /// <param name="eventSource">source to use for sample event creation - using this classes eventLog</param>
        /// <returns>true if log write OK</returns>
        private static bool AttemptCreateSampleEvent(string eventSource)
        {
            try
            {
                eventLog.WriteEntry(Properties.Settings.Default.DiagnosticsEventsource + " eventsource init...", EventLogEntryType.Information, 0);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// Log Event - call fuller overload with default output channels (for now app log and debug console)
        /// </summary>
        /// <param name="eventId">EventID - 0-65535</param>
        /// <param name="eventMessage">Event message</param>
        /// <param name="type">Event Log Entry Type</param>
        public static void LogEvent(int eventId, string eventMessage, EventLogEntryType type)
        {
            LogEvent(eventId, eventMessage, type, OutputChannels.ApplicationLog | OutputChannels.DebugConsole | OutputChannels.Slack);
        }

        /// <summary>
        /// Log Event - specify output channels for event...
        /// </summary>
        /// <param name="eventId">EventID - 0-65535</param>
        /// <param name="eventMessage">Event message</param>
        /// <param name="type">Event Log Entry Type</param>
        /// <param name="outputChannels">Output channels (bitwise enum) - application log, debug console, smtp mail, jabber etc.</param>
        public static void LogEvent(int eventId, string eventMessage, EventLogEntryType type, OutputChannels outputChannels)
        {
            if ((outputChannels & OutputChannels.ApplicationLog) == OutputChannels.ApplicationLog)
            {
                eventLog.WriteEntry(eventMessage, type, eventId);
            }
#if DEBUG
            if ((outputChannels & OutputChannels.DebugConsole) == OutputChannels.DebugConsole)
            {
                Debug.WriteLine(eventSource + " - " + eventId.ToString().PadLeft(5, '0') + " - " + DateTime.Now.ToString() + " - " + type.ToString() + " - " + eventMessage);
            }

            if ((outputChannels & OutputChannels.SmtpMail) == OutputChannels.SmtpMail)
            {
                Debug.WriteLine(eventId.ToString().PadLeft(5, '0') + " - " + DateTime.Now.ToString() + " - " + type.ToString() + " - " + eventMessage + " - NO SMTP CHANNEL IMPLEMENTED");
            }

            if ((outputChannels & OutputChannels.Jabber) == OutputChannels.Jabber)
            {
                Debug.WriteLine(eventId.ToString().PadLeft(5, '0') + " - " + DateTime.Now.ToString() + " - " + type.ToString() + " - " + eventMessage + " - NO JABBER CHANNEL IMPLEMENTED");
            }

#endif
            if ((outputChannels & OutputChannels.Slack) == OutputChannels.Slack)
            {
                if (slackClient != null)
                {
                    try
                    {
                        slackClient.PostMessage(eventSource + " - " + eventId.ToString().PadLeft(5, '0') + " - " + DateTime.Now.ToString() + " - " + type.ToString() + " - " + eventMessage);
                    }
                    catch (Exception slackEx)
                    {
                        Debug.WriteLine(eventSource + " - " + eventId.ToString().PadLeft(5, '0') + " - " + DateTime.Now.ToString() + " - " + type.ToString() + " - slack web hook exception: " + slackEx.Message);
                    }
                }
            }
        }



        /// <summary>
        /// enumeration of output channels to use (bitwise)
        /// </summary>
        [Flags]
        public enum OutputChannels
        {
            none = 0,
            ApplicationLog = 1,
            DebugConsole = 2,
            SmtpMail = 4,
            Jabber = 8,
            Slack = 16
        }
    }
}
