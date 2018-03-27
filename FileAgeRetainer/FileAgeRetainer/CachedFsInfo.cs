using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Security.Cryptography;
using static FileAgeRetainer.CommonComponents;
using Newtonsoft.Json;

namespace FileAgeRetainer
{
    /// <summary>
    /// Cached File System information record, these are added as value to the concurrent dictionary with a key of the full path of the fs object
    /// </summary>
    public class CachedFsInfo
    {
        private string _HashString;
        private string _FullPath;
        /// <summary>
        /// type of object for this cached item
        /// </summary>
        public FsObjType _type;
        private long _PreservationTimeStart;
        private long _LastTypeCheckTime;
        private bool _FsTypeChangeCausePreservationReset;
        private bool _FsChecksumChangeCausePreservationReset;
        private bool _FsRenameCausePreservationReset;
        private bool _FsHashCalculateWholeDirectoryHashes;
        private FsHashAlgorithm _FsHashAlgorithm;

        /// <summary>
        /// full path of the file system object (e.g. c:\temp\myfile.txt)
        /// </summary>
        public string FullPath
        {
            get
            {
                return _FullPath;
            }
            set { _FullPath = value; }
        }

        /// <summary>
        /// a string of the hash on the content checksum of the object.  directory hashes will be empty string unless option FsHashCalculateWholeDirectoryHashes is true
        /// </summary>
        public string HashString { get { return _HashString; } set { _HashString = value; } }

        /// <summary>
        /// the file system object type - file, directory or NotExist
        /// </summary>
        public FsObjType Type
        {
            get
            {
                if (IsTypeCheckStale())
                {
                    //its stale, recalculate type, if different, recalculate hash, if causes preservation reset do so
                    FsObjType newType = GetObjType(_FullPath);
                    if (newType != _type)
                    {
                        _type = newType;
                        _HashString = GetHashString(_FullPath, _type, _FsHashAlgorithm, _FsHashCalculateWholeDirectoryHashes);
                        if (_FsTypeChangeCausePreservationReset)
                        {
                            _PreservationTimeStart = _LastTypeCheckTime = DateTime.Now.Ticks;
                        }
                    }

                    _LastTypeCheckTime = DateTime.Now.Ticks;
                    return _type;
                }

                _LastTypeCheckTime = DateTime.Now.Ticks;
                return _type;
            }
        }

        /// <summary>
        /// determines if the type is stale, outside of FsTypeCheckToleranceSeconds, object will attempt to correct its object type when outside this tolerance.  useful when things go 'NotExist'
        /// </summary>
        /// <returns>true if type check out of tolerance time</returns>
        private bool IsTypeCheckStale()
        {
            if ((DateTime.Now.Ticks - _LastTypeCheckTime > Properties.Settings.Default.FsTypeCheckToleranceSeconds * TimeSpan.TicksPerSecond))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// the first time, in ticks, when the FileAgeRetainer saw this CachedFsInfo
        /// </summary>
        public long PreservationTimeStart { get { return _PreservationTimeStart; } set { _PreservationTimeStart = value; } }

        /// <summary>
        /// signal to this object that its checksum has changed, will update appropriate class instance variables / do hash string calc etc. remember to readd the transformed object back to dictionary if this is desired
        /// </summary>
        public void SignalChanged()
        {
            string oldHash = _HashString;
            string newHash = GetHashString(_FullPath, Type, _FsHashAlgorithm, _FsHashCalculateWholeDirectoryHashes);

            if (newHash != oldHash)
            {
                if (_FsChecksumChangeCausePreservationReset)
                {
                    _PreservationTimeStart = DateTime.Now.Ticks;
                }

                _HashString = newHash;
            }
        }

        /// <summary>
        /// signal to this object that it has been renamed
        /// </summary>
        public void SignalRenamed()
        {
            if (_FsRenameCausePreservationReset)
            {
                _PreservationTimeStart = DateTime.Now.Ticks;
            }
        }

        /// <summary>
        /// instantiate a CachedFsInfo
        /// </summary>
        /// <param name="fullPath">the full file system path of the fs object representing this CachedFsInfo</param>
        /// <param name="FsTypeChangeCausePreservationReset">true if type changes for an item will prompt a preservation reset</param>
        /// <param name="FsChecksumChangeCausePreservationReset">true if checksum change will cause a preservation reset</param>
        /// <param name="FsRenameCausePreservationReset">true if object rename will cause a preservation reset</param>
        /// <param name="hashAlgorithm">hash algorithm used for this CachedFsInfo</param>
        /// <param name="FsHashCalculateWholeDirectoryHashes">true if entire directory contents (with subtree) should be used for calculating checksum on this if a directory - EXPENSIVE</param>
        public CachedFsInfo(string fullPath, bool FsTypeChangeCausePreservationReset, bool FsChecksumChangeCausePreservationReset, bool FsRenameCausePreservationReset, FsHashAlgorithm hashAlgorithm, bool FsHashCalculateWholeDirectoryHashes)
        {
            _FullPath = fullPath;
            _type = GetObjType(fullPath);
            _FsTypeChangeCausePreservationReset = FsTypeChangeCausePreservationReset;
            _FsChecksumChangeCausePreservationReset = FsChecksumChangeCausePreservationReset;
            _FsRenameCausePreservationReset = FsRenameCausePreservationReset;
            _FsHashAlgorithm = hashAlgorithm;
            _FsHashCalculateWholeDirectoryHashes = FsHashCalculateWholeDirectoryHashes;

            if (_type != FsObjType.NotExist)
            {
                _HashString = GetHashString(fullPath, _type, _FsHashAlgorithm, _FsHashCalculateWholeDirectoryHashes);
            }
            else
            {
                _HashString = string.Empty;
            }

            _PreservationTimeStart = _LastTypeCheckTime = DateTime.Now.Ticks;
        }

        /// <summary>
        /// instantiate a CachedFsInfo object instance - bypasses any triggered calculations.  used for deserialization from disk cache
        /// </summary>
        /// <param name="fullPath">the full file system path of the fs object representing this CachedFsInfo</param>
        /// <param name="type">the FsObjType of the fs object that this cached record represents</param>
        /// <param name="hashString">the hash string of the fs object content this CachedFsInfo represents</param>
        /// <param name="PreservationTimeStart">time, in ticks, that this object was first seen by the service</param>
        /// <param name="FsTypeChangeCausePreservationReset">true if type changes for an item will prompt a preservation reset</param>
        /// <param name="FsChecksumChangeCausePreservationReset">true if checksum change will cause a preservation reset</param>
        /// <param name="FsRenameCausePreservationReset">true if object rename will cause a preservation reset</param>
        /// <param name="hashAlgorithm">hash algorithm used for this CachedFsInfo</param>
        /// <param name="FsHashCalculateWholeDirectoryHashes">true if entire directory contents (with subtree) should be used for calculating checksum on this if a directory - EXPENSIVE</param>
        [JsonConstructor]
        public CachedFsInfo(string fullPath, FsObjType type, string hashString, long PreservationTimeStart, bool FsTypeChangeCausePreservationReset, bool FsChecksumChangeCausePreservationReset, bool FsRenameCausePreservationReset, FsHashAlgorithm hashAlgorithm, bool FsHashCalculateWholeDirectoryHashes)
        {
            _FsHashCalculateWholeDirectoryHashes = FsHashCalculateWholeDirectoryHashes;
            _FsRenameCausePreservationReset = FsRenameCausePreservationReset;
            _FsChecksumChangeCausePreservationReset = FsChecksumChangeCausePreservationReset;
            _FsTypeChangeCausePreservationReset = FsTypeChangeCausePreservationReset;
            _FullPath = fullPath;
            _type = type;
            _HashString = hashString;
            _PreservationTimeStart = _LastTypeCheckTime = PreservationTimeStart;
            _FsHashAlgorithm = hashAlgorithm;
        }
    }
}
