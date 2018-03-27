using System;
using System.Text;
using Newtonsoft.Json;
using System.Collections.Specialized;
using System.Net;

namespace FileAgeRetainer
{


    /// <summary>
    /// A simple C# class to post messages to a Slack channel 
    /// Note: This class uses the Newtonsoft Json.NET serializer available via NuGet 
    /// </summary>
    public class SlackClient
    {
        private readonly Uri _uri;
        private readonly Encoding _encoding = new UTF8Encoding();

        /// <summary>
        /// instantiate slack client with web hook url to use
        /// </summary>
        /// <param name="urlWithAccessToken">web hook url to use</param>
        public SlackClient(string urlWithAccessToken)
        {
            _uri = new Uri(urlWithAccessToken);
        }


        /// <summary>
        /// Post a message using simple strings
        /// </summary>
        /// <param name="text">text of message</param>
        /// <param name="username">username, default is null</param>
        /// <param name="channel">channel, default is null</param>
        public void PostMessage(string text, string username = null, string channel = null)
        {
            Payload payload = new Payload()
            {
                Channel = channel,
                Username = username,
                Text = text
            };

            PostMessage(payload);
        }


        /// <summary>
        /// Post a message using a Payload object 
        /// </summary>
        /// <param name="payload">payload object</param>
        public void PostMessage(Payload payload)
        {
            string payloadJson = JsonConvert.SerializeObject(payload);

            using (WebClient client = new WebClient())
            {
                NameValueCollection data = new NameValueCollection();
                data["payload"] = payloadJson;

                var response = client.UploadValues(_uri, "POST", data);

                //The response text is usually "ok" 
                string responseText = _encoding.GetString(response);
            }
        }
    }



    /// <summary>
    /// This class serializes into the Json payload required by Slack Incoming WebHooks 
    /// </summary>
    public class Payload
    {
        /// <summary>
        /// slack channel to use
        /// </summary>
        [JsonProperty("channel")]
        public string Channel { get; set; }

        /// <summary>
        /// username to use
        /// </summary>
        [JsonProperty("username")]
        public string Username { get; set; }

        /// <summary>
        /// text of message
        /// </summary>
        [JsonProperty("text")]
        public string Text { get; set; }
    }

}
