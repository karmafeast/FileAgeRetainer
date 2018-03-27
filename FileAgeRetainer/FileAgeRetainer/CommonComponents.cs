using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Data.HashFunction;
using System.Management;
using System.Runtime.InteropServices;

namespace FileAgeRetainer
{
    /// <summary>
    /// Common components for the FileAgeRetainerService - e.g. hashing things, enums etc. used throughout service by other classes
    /// </summary>
    public static class CommonComponents
    {
        private static ParallelOptions _ParallelOptions;
        private static FsHashAlgorithm _HashAlgorithm;
        private static bool _HashWholeDirectories;
        private static string _ExecutingAssemblyDirectory;

        const int supportsCompression = 0x10;


        [DllImport("Kernl32.dll", SetLastError = true)]
        extern static bool GetVolumeInformation(string vol, StringBuilder name, int nameSize, out uint serialNum, out uint maxNameLen, out uint flags, StringBuilder fileSysName, int fileSysNameSize);

        /// <summary>
        /// Options for Parallel processes
        /// </summary>
        public static ParallelOptions ParallelOptions
        {
            get
            { return _ParallelOptions; }
        }

        /// <summary>
        /// hash algorithm used by default - typically set from global options, and used when certain overloads for calculating hashes are invoked
        /// </summary>
        public static FsHashAlgorithm HashAlgorithm
        { get { return _HashAlgorithm; } }

        /// <summary>
        /// true if should calculate directory subtree content hashes on a directory - EXPENSIVE - typically set from global options, and used when certain overloads for calculating hashes are invoked
        /// </summary>
        public static bool HashWholeDirectories { get { return _HashWholeDirectories; } }

        /// <summary>
        /// the directory in which this assembly is executing, used for where to locate serialized caches for FileAgeRetainerItem
        /// </summary>
        public static string ExecutingAssemblyDirectory { get { return _ExecutingAssemblyDirectory; } }

        /// <summary>
        /// enumeration for the type of fs object a cached record represents
        /// </summary>
        public enum FsObjType
        {   /// <summary>
            /// a file 
            /// </summary>
            File,
            /// <summary>
            /// a directory
            /// </summary>
            Directory,
            /// <summary>
            /// a non-existent object
            /// </summary>
            NotExist
        }

        /// <summary>
        /// enumeration for the hashing algorithm to use - xxHash is fastest
        /// </summary>
        public enum FsHashAlgorithm
        {   /// <summary>
            /// xxHash algorithm
            /// </summary>
            xxHash,
            /// <summary>
            /// SHA1 algorithm
            /// </summary>
            SHA1,
            /// <summary>
            /// MD5 algorithm
            /// </summary>
            MD5
        }


        /// <summary>
        /// get the object type from a full path of an fs object
        /// </summary>
        /// <param name="fullPath">the full path of the fs object to get the type of</param>
        /// <returns>type of object the specified path equates to</returns>
        public static FsObjType GetObjType(string fullPath)
        {
            if (Directory.Exists(fullPath)) { return FsObjType.Directory; } else if (File.Exists(fullPath)) { return FsObjType.File; } else { return FsObjType.NotExist; }
        }


        /// <summary>
        /// get a hash of a string - can be whatever input - used for serialization dictionary naming
        /// </summary>
        /// <param name="dataToHash">string of data to hash</param>
        /// <param name="hashAlgorithm">hash algorithm to use</param>
        /// <returns>hashed string</returns>
        public static string GetHashString(string dataToHash, FsHashAlgorithm hashAlgorithm)
        {
            switch (hashAlgorithm)
            {
                case FsHashAlgorithm.xxHash:
                    xxHash _xxHash = new xxHash(64);
                    return BitConverter.ToString(_xxHash.ComputeHash(dataToHash, 64)).Replace("-", "").ToLower();
                case FsHashAlgorithm.SHA1:
                    SHA1 sha = SHA1.Create();
                    byte[] arrayForSha = Encoding.ASCII.GetBytes(dataToHash);
                    return BitConverter.ToString(sha.ComputeHash(arrayForSha)).Replace("-", "").ToLower();
                case FsHashAlgorithm.MD5:
                    MD5Cng md5 = new MD5Cng();
                    byte[] arrayForMd5 = Encoding.ASCII.GetBytes(dataToHash);
                    return BitConverter.ToString(md5.ComputeHash(arrayForMd5)).Replace("-", "").ToLower();
                default:
                    return string.Empty;
            }
        }

        /// <summary>
        /// get hash of fs Object using specified options
        /// </summary>
        /// <param name="fullPath">full path to fs object</param>
        /// <param name="type">object type - file, directory, notExist</param>
        /// <param name="hashAlgorithm">hash algorithm to use</param>
        /// <param name="hashWholeDirectories">calculate hashes of entire directory subtrees - EXPENSIVE</param>
        /// <returns>hash string</returns>
        public static string GetHashString(string fullPath, FsObjType type, FsHashAlgorithm hashAlgorithm, bool hashWholeDirectories)
        {
            switch (hashAlgorithm)
            {
                case FsHashAlgorithm.xxHash:
                    switch (type)
                    {
                        case FsObjType.File:
                            {
                                try
                                {
                                    using (FileStream fs = File.OpenRead(fullPath))
                                    {
                                        xxHash _xxHash = new xxHash(64);
                                        return BitConverter.ToString(_xxHash.ComputeHash(fs)).Replace("-", "").ToLower();
                                    }
                                }
                                catch (IOException e)
                                {
#if DEBUG
                                    DiagnosticsEventHandler.LogEvent(100, e.Message, EventLogEntryType.Warning, DiagnosticsEventHandler.OutputChannels.DebugConsole | DiagnosticsEventHandler.OutputChannels.Slack);
#endif
                                    return "IOException";
                                }
                            }
                        case FsObjType.Directory:
                            if (hashWholeDirectories)
                            {
                                SHA1 _SHA1 = SHA1.Create();
                                //NOTE - for now xxHash will use sha1 for whole directory implementation
                                var files = GetFsItemList(fullPath, type, true, "*.*", false, true);

                                for (int i = 0; i < files.Count; i++)
                                {
                                    string file = files[i];

                                    // hash path
                                    string relativePath = file.Substring(fullPath.Length + 1);
                                    byte[] pathBytes = Encoding.UTF8.GetBytes(relativePath.ToLower());
                                    _SHA1.TransformBlock(pathBytes, 0, pathBytes.Length, pathBytes, 0);

                                    // hash contents
                                    byte[] contentBytes = File.ReadAllBytes(file);
                                    if (i == files.Count - 1)
                                        _SHA1.TransformFinalBlock(contentBytes, 0, contentBytes.Length);
                                    else
                                        _SHA1.TransformBlock(contentBytes, 0, contentBytes.Length, contentBytes, 0);
                                }

                                return BitConverter.ToString(_SHA1.Hash).Replace("-", "").ToLower();
                            }
                            //if not hashing whole directories just return an empty string
                            return String.Empty;
                        case FsObjType.NotExist:
                            return string.Empty;
                        default:
                            return string.Empty;
                    }
                case FsHashAlgorithm.SHA1:
                    switch (type)
                    {
                        case FsObjType.File:
                            try
                            {
                                using (FileStream fs = File.OpenRead(fullPath))
                                {
                                    SHA1 sha = SHA1.Create();
                                    return BitConverter.ToString(sha.ComputeHash(fs)).Replace("-", "").ToLower();
                                }
                            }
                            catch (IOException e)
                            {
#if DEBUG
                                DiagnosticsEventHandler.LogEvent(101, e.Message, EventLogEntryType.Warning, DiagnosticsEventHandler.OutputChannels.DebugConsole | DiagnosticsEventHandler.OutputChannels.Slack);
#endif
                                return "IOException";
                            }
                        case FsObjType.Directory:
                            if (hashWholeDirectories)
                            {
                                //VERY EXPENSIVE TO CALCULATE OVERALL HASH ON FOLDERS -just treat them as stubs with no hash we care about
                                //assuming we want to include nested folders
                                var files = GetFsItemList(fullPath, type, true, "*.*", false, true);

                                SHA1 sha1 = SHA1.Create();

                                for (int i = 0; i < files.Count; i++)
                                {
                                    string file = files[i];

                                    // hash path
                                    string relativePath = file.Substring(fullPath.Length + 1);
                                    byte[] pathBytes = Encoding.UTF8.GetBytes(relativePath.ToLower());
                                    sha1.TransformBlock(pathBytes, 0, pathBytes.Length, pathBytes, 0);

                                    // hash contents
                                    byte[] contentBytes = File.ReadAllBytes(file);
                                    if (i == files.Count - 1)
                                        sha1.TransformFinalBlock(contentBytes, 0, contentBytes.Length);
                                    else
                                        sha1.TransformBlock(contentBytes, 0, contentBytes.Length, contentBytes, 0);
                                }

                                return BitConverter.ToString(sha1.Hash).Replace("-", "").ToLower();
                            }
                            //if not hashing whole directories just return an empty string
                            return String.Empty;
                        case FsObjType.NotExist:
                            return String.Empty;
                        default:
                            return string.Empty;
                    }
                case FsHashAlgorithm.MD5:
                    switch (type)
                    {
                        case FsObjType.File:
                            try
                            {
                                using (FileStream fs = File.OpenRead(fullPath))
                                {
                                    MD5Cng md5 = new MD5Cng();
                                    return BitConverter.ToString(md5.ComputeHash(fs)).Replace("-", "").ToLower();
                                }
                            }
                            catch (IOException e)
                            {
#if DEBUG
                                DiagnosticsEventHandler.LogEvent(102, e.Message, EventLogEntryType.Warning, DiagnosticsEventHandler.OutputChannels.DebugConsole);
#endif
                                return "IOException";
                            }
                        case FsObjType.Directory:
                            if (hashWholeDirectories)
                            {
                                //VERY EXPENSIVE TO CALCULATE OVERALL HASH ON FOLDERS -just treat them as stubs with no hash we care about
                                //assuming we want to include nested folders
                                var files = GetFsItemList(fullPath, type, true, "*.*", false, true);

                                MD5 md5 = MD5.Create();

                                for (int i = 0; i < files.Count; i++)
                                {
                                    string file = files[i];

                                    // hash path
                                    string relativePath = file.Substring(fullPath.Length + 1);
                                    byte[] pathBytes = Encoding.UTF8.GetBytes(relativePath.ToLower());
                                    md5.TransformBlock(pathBytes, 0, pathBytes.Length, pathBytes, 0);

                                    // hash contents
                                    byte[] contentBytes = File.ReadAllBytes(file);
                                    if (i == files.Count - 1)
                                        md5.TransformFinalBlock(contentBytes, 0, contentBytes.Length);
                                    else
                                        md5.TransformBlock(contentBytes, 0, contentBytes.Length, contentBytes, 0);
                                }

                                return BitConverter.ToString(md5.Hash).Replace("-", "").ToLower();
                            }
                            //if not hashing whole directories just return an empty string
                            return String.Empty;
                        case FsObjType.NotExist:
                            return String.Empty;
                        default:
                            return string.Empty;
                    }
                default:
                    return string.Empty;
            }

        }

        /// <summary>
        /// get hash of fs Object using global defaults, while specifying the object type (do this if already known, cheaper)
        /// </summary>
        /// <param name="fullPath">full fs path to object to be hashed</param>
        /// <param name="type">object type - file, directory, notExist</param>
        /// <returns>hash string in the global default hash algorithm</returns>
        public static string GetHashString(string fullPath, FsObjType type)
        {
            return GetHashString(fullPath, type, _HashAlgorithm, _HashWholeDirectories);
        }


        /// <summary>
        /// get hash of fs Object using global defaults, Determine the object type now (do this only if obj type not known)
        /// </summary>
        /// <param name="fullPath">full fs path to object to be hashed</param>
        /// <returns>hash string in the global default hash algorithm</returns>
        public static string GetHashString(string fullPath)
        {
            return GetHashString(fullPath, GetObjType(fullPath), _HashAlgorithm, _HashWholeDirectories);
        }

        /// <summary>
        /// NOT YET IMPLEMENTED - compress a folder using transparent compression
        /// </summary>
        /// <param name="directory">the full path of the directory to compress</param>
        /// <param name="recursive">perform action on subtree of root if true</param>
        /// <returns></returns>
        public static uint CompressFolder(DirectoryInfo directory, bool recursive)
        {

            string path = "Win32_Directory.Name='" + directory.FullName + "'";
            using (ManagementObject dir = new ManagementObject(path))
            using (ManagementBaseObject p = dir.GetMethodParameters("CompressEx"))
            {
                p["Recursive"] = recursive;
                using (ManagementBaseObject result = dir.InvokeMethod("CompressEx", p, null))
                    return (uint)result.Properties["ReturnValue"].Value;
            }
        }

        /// <summary>
        /// NOT YET IMPLEMENTED - uncompress a directory using NTFS transparent compression
        /// </summary>
        /// <param name="directory">the path of the directory to uncompress</param>
        /// <param name="recursive">perform the operation on subtree if true</param>
        /// <returns>return code from operation</returns>
        public static uint UncompressFolder(DirectoryInfo directory, bool recursive)
        {
            string path = "Win32_Directory.Name='" + directory.FullName + "'";
            using (ManagementObject dir = new ManagementObject(path))
            using (ManagementBaseObject p = dir.GetMethodParameters("UncompressEx"))
            {
                p["Recursive"] = recursive;
                using (ManagementBaseObject result = dir.InvokeMethod("UncompressEx", p, null))
                    return (uint)result.Properties["ReturnValue"].Value;
            }
        }

        /// <summary>
        /// NOT YET IMPLEMENTED - determine if the volume file system supports transparent compression
        /// </summary>
        /// <param name="volumeToCheck">volume to check</param>
        /// <returns>true if volume file system supports transparent compression</returns>
        public static bool VolumeSupportsCompression(string volumeToCheck)
        {
            uint serialNum, maxNameLen, flags;
            bool ok = GetVolumeInformation(volumeToCheck, null, 0, out serialNum, out maxNameLen, out flags, null, 0);
            if (!ok) { throw new ApplicationException("VolumeSupportsCompression exception GetVolumeInformation on " + volumeToCheck); }
            return (flags & supportsCompression) != 0;
        }

        /// <summary>
        /// determine if a given object is compressed with transparent compression via flags on object
        /// </summary>
        /// <param name="fullPath">full path to object to check</param>
        /// <returns>true if object is compressed with transparent compression as indicated by object flags</returns>
        public static bool IsFsObjectCompressed(string fullPath)
        {
            switch (GetObjType(fullPath))
            {
                case FsObjType.File:
                    return new FileInfo(fullPath).Attributes.HasFlag(FileAttributes.Compressed);
                case FsObjType.Directory:
                    return new DirectoryInfo(fullPath).Attributes.HasFlag(FileAttributes.Compressed);
                case FsObjType.NotExist:
                    throw new ArgumentException("fsObject " + fullPath + " does not exist");
                default:
                    throw new ApplicationException("logic error in IsFsObjectCompressed");
            }
        }

        /// <summary>
        /// get a list of files / directories within a fs search path
        /// </summary>
        /// <param name="searchRoot">full path of where to search for file system objects</param>
        /// <param name="rootType">the type of the root object - file or directory</param>
        /// <param name="subtree">search the subtree of a directory if true</param>
        /// <param name="filter">the filter e.g. *.* for the search</param>
        /// <param name="includeDirectories">include directories in the results, false will not return any directories under the search root</param>
        /// <param name="includeFiles">include files in the results, false will not return any files under the search root</param>
        /// <returns>a list of files / directories under the search root considering passed params</returns>
        public static List<string> GetFsItemList(string searchRoot, FsObjType rootType, bool subtree, string filter, bool includeDirectories, bool includeFiles)
        {
            List<string> files = new List<string>();
            List<string> directories = new List<string>();

            switch (rootType)
            {
                case FsObjType.File:
                    string terminalElement = searchRoot.Split('\\')[searchRoot.Split('\\').Length - 1];
                    string pathElement = searchRoot.Remove(searchRoot.LastIndexOf('\\'));
                    files = Directory.GetFiles(pathElement, terminalElement, SearchOption.TopDirectoryOnly).ToList();
                    break;
                case FsObjType.Directory:
                    if (subtree)
                    {
                        if (includeFiles)
                        {
                            files = Directory.GetFiles(searchRoot, filter, SearchOption.AllDirectories)
                                     .OrderBy(p => p).ToList();
                        }
                        if (includeDirectories)
                        {
                            directories = Directory.GetDirectories(searchRoot, "*", SearchOption.AllDirectories).OrderBy(p => p).ToList();
                            files.AddRange(directories);
                        }
                    }
                    else
                    {
                        if (includeFiles)
                        {
                            files = Directory.GetFiles(searchRoot, filter, SearchOption.TopDirectoryOnly)
                                 .OrderBy(p => p).ToList();
                        }
                        if (includeDirectories)
                        {
                            directories = Directory.GetDirectories(searchRoot, "*", SearchOption.TopDirectoryOnly).OrderBy(p => p).ToList();
                            files.AddRange(directories);
                        }
                    }
                    break;
                case FsObjType.NotExist:
                    DiagnosticsEventHandler.LogEvent(60066, "ERROR - Attempt get fs contents on non existent target: " + searchRoot, EventLogEntryType.Error);
                    throw new ApplicationException("Attempt get fs contents on non existent target: " + searchRoot);
                default:
                    DiagnosticsEventHandler.LogEvent(60067, "ERROR - bad logic in fs search: " + searchRoot, EventLogEntryType.Error);
                    throw new ApplicationException("bad logic in fs search: " + searchRoot);
            }
            return files;
        }

        /// <summary>
        /// instantiation of the CommonComponents class
        /// </summary>
        static CommonComponents()
        {
            _ParallelOptions = new ParallelOptions();
            _ParallelOptions.MaxDegreeOfParallelism = Properties.Settings.Default.CacheWorkerBlockMaxParrallelism;
            _HashWholeDirectories = Properties.Settings.Default.FsHashCalculateWholeDirectoryHashes;
            bool configHashAlgOk = Enum.TryParse<FsHashAlgorithm>(Properties.Settings.Default.FsHashAlgorithm, out _HashAlgorithm);
            if (!configHashAlgOk) { DiagnosticsEventHandler.LogEvent(900, "could not parse desired hash algorithm from app.config: " + Properties.Settings.Default.FsHashAlgorithm, EventLogEntryType.Warning); _HashAlgorithm = FsHashAlgorithm.xxHash; }
            _ExecutingAssemblyDirectory = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
        }

    }
}
