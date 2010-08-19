using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Win32;
using System.Data;
using System.Data.Sql;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.IO;
using System.Threading;
using System.IO.Compression;
using System.Text.RegularExpressions;
using Dokan;


namespace MSSQLFS
{
    class FileCaching
    {
        public MemoryStream MemStream;
        public FileInformation FileInfo;
        
    }


    class MSSQLFS : DokanOperations
    {

        private Dictionary<String, FileCaching> FileCache = new Dictionary<String, FileCaching>();

        public String ZippedExtension = " .zip .gzip .tar .arj .7z .7zip .rar .gif .jpg .jpeg ";

        string ConnectionString;

        #region DokanOperations member


        public MSSQLFS()
        {
            ConnectionString = "";
        }

        public MSSQLFS(String ConnString)
        {
            ConnectionString = ConnString;
        }

        #region Directory function
        public int CreateDirectory(string filename, DokanFileInfo info)
        {
            //create directory in database
            using (SqlConnection conn = new SqlConnection(ConnectionString))
            {
                SqlCommand SP = new SqlCommand();
                SP.Connection = conn;
                SP.CommandType = CommandType.StoredProcedure;
                SP.CommandText = "CreateDirectory";
                SP.Parameters.AddWithValue("@filename", filename);
                conn.Open();
                try
                {
                    SP.ExecuteNonQuery(); //on MoveFile can raise error due directory is exists
                }
                catch
                {
                }
            }
            return DokanNet.DOKAN_SUCCESS;
        }

        public int DeleteDirectory(string filename, DokanFileInfo info)
        {
            return DeleteFile(filename, info);
        }

        public int OpenDirectory(string filename, DokanFileInfo info)
        {
            return DokanNet.DOKAN_SUCCESS;
        }

        #endregion


        private static void Decompress(MemoryStream zipped, MemoryStream Output)
        {
            zipped.Seek(0, SeekOrigin.Begin);
            GZipStream gzip = new GZipStream(zipped, CompressionMode.Decompress, true);

            byte[] bytes = new byte[4096];
            int n;
            while ((n = gzip.Read(bytes, 0, bytes.Length)) != 0)
            {
                Output.Write(bytes, 0, n);
            }
            gzip.Close();
        }

        private static void Compress(MemoryStream raw, MemoryStream Output)
        {
            GZipStream gzip = new GZipStream(Output, CompressionMode.Compress, true);
            raw.Seek(0, SeekOrigin.Begin);
            byte[] bytes = new byte[4096];
            int n;
            while ((n = raw.Read(bytes, 0, bytes.Length)) != 0)
            {
                gzip.Write(bytes, 0, n);
            }
            gzip.Close();
        }




        public int Cleanup(string filename, DokanFileInfo info)
        {
            lock (FileCache)
            {
                if ((FileCache.ContainsKey(filename) == true) && (FileCache[filename].MemStream.Length > 0))
                {
                    using (SqlConnection conn = new SqlConnection(ConnectionString))
                    {
                        using (SqlCommand Cmd = new SqlCommand())
                        {
                            MemoryStream mem = ((FileCaching)FileCache[filename]).MemStream;
                            Cmd.CommandText = "WriteFile";
                            Cmd.Parameters.Add("@iszipped", SqlDbType.Bit, 1);
                            Cmd.Parameters["@iszipped"].Value = 0;
                            Cmd.Parameters.Add("@OriginalSize", SqlDbType.BigInt);
                            Cmd.Parameters["@OriginalSize"].Value = mem.Length;

                            if (this.ZippedExtension.ToLower().IndexOf(Path.GetExtension(Regex.Split(filename.ToLower(), ".version")[0])) == -1)
                            {
                                if (FileCache[filename].MemStream.Length > 256)
                                {
                                    Cmd.Parameters["@iszipped"].Value = 1;
                                    MemoryStream dummy = new MemoryStream();
                                    Compress(mem, dummy);
                                    mem.SetLength(0);
                                    dummy.WriteTo(mem);
                                }
                            }

                            mem.Seek(0, SeekOrigin.Begin);
                            Cmd.Parameters.Add("@data", SqlDbType.VarBinary, (int)mem.Length);
                            Cmd.Parameters["@data"].SqlValue = mem.ToArray();

                            Cmd.Parameters.AddWithValue("@filename", filename);

                            Cmd.CommandType = CommandType.StoredProcedure;
                            Cmd.Connection = conn;
                            conn.Open();

                            Cmd.ExecuteNonQuery();
                            FileCache.Remove(filename);
                        }
                    }
                }
            };
            return DokanNet.DOKAN_SUCCESS;
        }

        public int CloseFile(string filename, DokanFileInfo info)
        {
            return DokanNet.DOKAN_SUCCESS;
        }


        public int CreateFile(string filename, System.IO.FileAccess access, System.IO.FileShare share, System.IO.FileMode mode, System.IO.FileOptions options, DokanFileInfo info)
        {
            FileInformation fi = new FileInformation();
            GetFileInformation(filename, ref fi, info);

            switch (mode)
            {
                case FileMode.Append:
                    return DokanNet.DOKAN_SUCCESS;
                case FileMode.Create:
                    AddToFileCache(filename, FillFileCache(filename));
                    return DokanNet.DOKAN_SUCCESS;
                case FileMode.CreateNew:
                    AddToFileCache(filename, FillFileCache(filename));
                    return DokanNet.DOKAN_SUCCESS;
                case FileMode.Open:
                    return DokanNet.DOKAN_SUCCESS;
                case FileMode.Truncate:
                    return DokanNet.DOKAN_SUCCESS;
            }
            return DokanNet.DOKAN_ERROR;
        }


        public int DeleteFile(string filename, DokanFileInfo info)
        {
            using (SqlConnection conn = new SqlConnection(ConnectionString))
            {
                SqlCommand Cmd = new SqlCommand();
                Cmd.CommandText = "DeleteFile";
                Cmd.Parameters.AddWithValue("@filename", filename);
                Cmd.CommandType = CommandType.StoredProcedure;
                Cmd.Connection = conn;
                conn.Open();
                Cmd.ExecuteNonQuery(); //TODO:react on error on SQL side
                FileCache.Remove(filename);
            }
            return DokanNet.DOKAN_SUCCESS;
        }


        public int FlushFileBuffers(string filename, DokanFileInfo info)
        {
            return CloseFile(filename, info);
        }


        public int FindFiles(string filename, System.Collections.ArrayList files, DokanFileInfo info)
        {
            using (SqlConnection conn = new SqlConnection(ConnectionString))
            {
                SqlCommand Cmd = new SqlCommand();
                Cmd.CommandText = "FindFiles";
                Cmd.Parameters.AddWithValue("@filename", filename);
                Cmd.CommandType = CommandType.StoredProcedure;
                Cmd.Connection = conn;
                conn.Open();
                using (SqlDataReader reader = Cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        FileInformation finfo = new FileInformation();
                        finfo.FileName = reader[0].ToString();
                        finfo.Attributes = reader[1].ToString() == "True" ? FileAttributes.Directory : FileAttributes.Normal;
                        lock (FileCache)
                        {
                            if (FileCache.ContainsKey(finfo.FileName) == true)
                            {
                                finfo.LastAccessTime = FileCache[finfo.FileName].FileInfo.LastAccessTime;
                                finfo.CreationTime = FileCache[finfo.FileName].FileInfo.CreationTime;
                                finfo.LastWriteTime = FileCache[finfo.FileName].FileInfo.LastWriteTime;
                                finfo.Length = FileCache[finfo.FileName].FileInfo.Length;
                            }
                            else
                            {
                                DateTime.TryParse(reader[4].ToString(), out finfo.LastAccessTime);
                                DateTime.TryParse(reader[5].ToString(), out finfo.LastWriteTime);
                                DateTime.TryParse(reader[6].ToString(), out finfo.CreationTime);
                                finfo.Length = (reader[2] is DBNull) ? 0 : int.Parse(reader[2].ToString());
                            }
                        }
                        files.Add(finfo);
                    }
                }
                conn.Close();
            }
            return DokanNet.DOKAN_SUCCESS;
        }


        public int GetFileInformation(string filename, ref FileInformation fileinfo, DokanFileInfo info)
        {
            lock (FileCache)
            {
                if (FileCache.ContainsKey(filename) == false)
                {
                    int RetVal = AddToFileCache(filename);
                    if (RetVal == DokanNet.DOKAN_SUCCESS)
                    {
                        fileinfo = FileCache[filename].FileInfo;
                    }
                    return RetVal;
                }
                else
                {
                    fileinfo = FileCache[filename].FileInfo;
                }
            }
            return DokanNet.DOKAN_SUCCESS;
        }


        public int LockFile(string filename, long offset, long length, DokanFileInfo info)
        {
            return DokanNet.DOKAN_SUCCESS;
        }

        public int MoveFile(string filename, string newname, bool replace, DokanFileInfo info)
        {
            using (SqlConnection conn = new SqlConnection(ConnectionString))
            {
                SqlCommand Cmd = new SqlCommand();
                Cmd.CommandText = "MoveFile";
                Cmd.Parameters.AddWithValue("@filename", filename);
                Cmd.Parameters.AddWithValue("@newname", newname);
                Cmd.Parameters.AddWithValue("@replace", replace);
                Cmd.CommandType = CommandType.StoredProcedure;
                Cmd.Connection = conn;
                conn.Open();
                Cmd.ExecuteNonQuery(); //TODO:react on error
                
                FileCaching fc = FileCache[filename];
                FileCache.Remove(filename);
                FileCache.Remove(newname);
            }
            return DokanNet.DOKAN_SUCCESS;

        }

        public int ReadFile(string filename, byte[] buffer, ref uint readBytes, long offset, DokanFileInfo info)
        {
            lock (FileCache)
            {
                if (FileCache.ContainsKey(filename) == false)
                {
                    return -1 * DokanNet.ERROR_FILE_NOT_FOUND;
                }

                if (FileCache[filename].MemStream.Length == 0)
                {
                    long readed = -1;

                    using (SqlConnection conn = new SqlConnection(ConnectionString))
                    {
                        SqlCommand Cmd = new SqlCommand();
                        Cmd.CommandText = "ReadFile";
                        Cmd.Parameters.AddWithValue("@filename", filename);

                        Cmd.CommandType = CommandType.StoredProcedure;
                        Cmd.Connection = conn;
                        conn.Open();
                        using (SqlDataReader reader = Cmd.ExecuteReader())
                        {
                            reader.Read();
                            readed = (long)reader[0];
                            FileCache[filename].MemStream = new MemoryStream();
                            FileCache[filename].FileInfo.LastAccessTime = DateTime.Now;
                            FileCache[filename].MemStream.Write((reader[2] as byte[]), 0, (int)readed);

                            bool IsZipped = !(reader[1] is DBNull) ? (bool)reader[1] : false;
                            if (IsZipped)
                            {
                                MemoryStream mem2 = new MemoryStream();
                                Decompress(FileCache[filename].MemStream, mem2);
                                FileCache[filename].MemStream.SetLength(0);
                                mem2.WriteTo(FileCache[filename].MemStream);
                            }
                        }
                    }
                }

                FileCache[filename].MemStream.Seek(offset, SeekOrigin.Begin);
                readBytes = (uint)FileCache[filename].MemStream.Read(buffer, 0, buffer.Length);
                if ((offset == FileCache[filename].MemStream.Length) && (readBytes == 0))
                {
                    return (-1);
                }
            };
            return DokanNet.DOKAN_SUCCESS;
        }

        public int WriteFile(string filename, byte[] buffer, ref uint writtenBytes, long offset, DokanFileInfo info)
        {
            lock (FileCache)
            {
                FileCache[filename].MemStream.Seek(offset, SeekOrigin.Begin);
                FileCache[filename].MemStream.Write(buffer, 0, (int)buffer.Length);

                writtenBytes = (uint)buffer.Length;
                FileCache[filename].FileInfo.LastWriteTime = DateTime.Now;
                FileCache[filename].FileInfo.Length = FileCache[filename].MemStream.Length;
            };
            return DokanNet.DOKAN_SUCCESS;
        }

        public int SetEndOfFile(string filename, long length, DokanFileInfo info)
        {
            lock (FileCache)
            {
                if (FileCache.ContainsKey(filename) == true)
                {
                    FileCache[filename].MemStream.SetLength(length);
                }
            }
            return DokanNet.DOKAN_SUCCESS;
        }

        public int SetAllocationSize(string filename, long length, DokanFileInfo info)
        {
            return SetEndOfFile(filename, length, info);
        }

        public int SetFileAttributes(string filename, System.IO.FileAttributes attr, DokanFileInfo info)
        {
            return DokanNet.DOKAN_SUCCESS;
        }

        public int SetFileTime(string filename, DateTime ctime, DateTime atime, DateTime mtime, DokanFileInfo info)
        {
            lock (FileCache)
            {
                if (FileCache.ContainsKey(filename) == true)
                {
                    FileCache[filename].FileInfo.LastAccessTime = atime;
                    FileCache[filename].FileInfo.CreationTime = ctime;
                    FileCache[filename].FileInfo.LastWriteTime = mtime;
                }
            }
            return DokanNet.DOKAN_SUCCESS;
        }

        public int UnlockFile(string filename, long offset, long length, DokanFileInfo info)
        {
            return DokanNet.DOKAN_SUCCESS;
        }

        public int Unmount(DokanFileInfo info)
        {
            //TODO: flush all opened files to Sql
            FileCache.Clear();
            return DokanNet.DOKAN_SUCCESS;
        }

        public int GetDiskFreeSpace(ref ulong freeBytesAvailable, ref ulong totalBytes, ref ulong totalFreeBytes, DokanFileInfo info)
        {
            freeBytesAvailable = 512 * 1024 * 1024;
            totalBytes = 1024 * 1024 * 1024;
            totalFreeBytes = 512 * 1024 * 1024;
            return DokanNet.DOKAN_SUCCESS;
        }


        private int AddToFileCache(string filename)
        {
            lock (FileCache)
            {
                if (FileCache.ContainsKey(filename) == false)
                {
                    FileCaching fc = FillFileCache(filename);

                    using (SqlConnection conn = new SqlConnection(ConnectionString))
                    {
                        using (SqlCommand Cmd = new SqlCommand())
                        {
                            Cmd.CommandText = "GetFileInformation";
                            Cmd.Parameters.AddWithValue("@filename", filename);
                            Cmd.Parameters.Add("@IsDirectory", SqlDbType.Bit);
                            Cmd.Parameters["@IsDirectory"].Direction = ParameterDirection.Output;
                            Cmd.Parameters.Add("@Length", SqlDbType.BigInt);
                            Cmd.Parameters["@Length"].Direction = ParameterDirection.Output;
                            Cmd.Parameters.Add("@LastAccessTime", SqlDbType.DateTime);
                            Cmd.Parameters["@LastAccessTime"].Direction = ParameterDirection.Output;
                            Cmd.Parameters.Add("@LastWriteTime", SqlDbType.DateTime);
                            Cmd.Parameters["@LastWriteTime"].Direction = ParameterDirection.Output;
                            Cmd.Parameters.Add("@CreationTime", SqlDbType.DateTime);
                            Cmd.Parameters["@CreationTime"].Direction = ParameterDirection.Output;
                            Cmd.CommandType = CommandType.StoredProcedure;
                            Cmd.Connection = conn;
                            conn.Open();

                            Cmd.ExecuteNonQuery();
                            if (Cmd.Parameters["@CreationTime"].Value is System.DBNull)
                            {
                                return -1 * DokanNet.ERROR_FILE_NOT_FOUND;
                            }
                            
                            fc.FileInfo.FileName = filename;
                            fc.FileInfo.Attributes = (Cmd.Parameters["@IsDirectory"].Value.ToString() == "True") ? System.IO.FileAttributes.Directory : System.IO.FileAttributes.Normal;

                            DateTime.TryParse(Cmd.Parameters["@LastAccessTime"].Value.ToString(), out fc.FileInfo.LastAccessTime);
                            DateTime.TryParse(Cmd.Parameters["@LastWriteTime"].Value.ToString(), out fc.FileInfo.LastWriteTime);
                            DateTime.TryParse(Cmd.Parameters["@CreationTime"].Value.ToString(), out fc.FileInfo.CreationTime);

                            fc.FileInfo.Length = Cmd.Parameters["@Length"].Value is System.DBNull ? 0 : (Int64)Cmd.Parameters["@Length"].Value;
                            FileCache.Add(filename, fc);
                        }
                    }
                }
            }
            return DokanNet.DOKAN_SUCCESS;
        }

        private void AddToFileCache(string filename, FileCaching fc)
        {
            lock (FileCache)
            {
                if (FileCache.ContainsKey(filename) == false)
                {
                    FileCache.Add(filename, fc);
                }
            }
        }

        private static FileCaching FillFileCache(String filename)
        {
            FileCaching fc = new FileCaching();
            fc.MemStream = new MemoryStream();
            fc.FileInfo = new FileInformation();
            fc.FileInfo.CreationTime = DateTime.Now;
            fc.FileInfo.LastAccessTime = DateTime.Now;
            fc.FileInfo.LastWriteTime = DateTime.Now;
            fc.FileInfo.Length = 0;
            fc.FileInfo.FileName = filename;
            return fc;
        }


        #endregion
    }
}
