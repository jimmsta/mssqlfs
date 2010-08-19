using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using Dokan;

/*
  TODO:attributes, lastwrite/read/access
 *     react on non connected database
 
 */


namespace MSSQLFS
{
   
    class Program
    {

        static void Main(string[] args)
        {
            DokanOptions opt = new DokanOptions();
            opt.DriveLetter = 'r';
            opt.NetworkDrive = true;
            opt.DebugMode = false;
            opt.UseAltStream = false;
            opt.UseKeepAlive = true;
            opt.UseStdErr = true;
            opt.VolumeLabel = "MSSQLFS";

            String Server = "", Database = "", User = "", Password = "";
            opt.DriveLetter = (System.Configuration.ConfigurationSettings.AppSettings["drive"] != null) ?System.Configuration.ConfigurationSettings.AppSettings["drive"][0] : 'r';
            Server = System.Configuration.ConfigurationSettings.AppSettings["server"];
            Database = System.Configuration.ConfigurationSettings.AppSettings["database"];
            User = System.Configuration.ConfigurationSettings.AppSettings["user"];
            Password = System.Configuration.ConfigurationSettings.AppSettings["password"];
            foreach (String arg in args)
            {
                if (Regex.Match(arg, "/drive:.").Success)
                    opt.DriveLetter = Regex.Split(arg, "/drive:")[1][0];
                if (Regex.Match(arg, "/server:*").Success)
                    Server = Regex.Split(arg, "/server:")[1];
                if (Regex.Match(arg, "/database:*").Success)
                    Database = Regex.Split(arg, "/database:")[1];
                if (Regex.Match(arg, "/user:*").Success)
                    User = Regex.Split(arg, "/user:")[1];
                if (Regex.Match(arg, "/password:*").Success)
                    Password = Regex.Split(arg, "/password:")[1];
            }

            String ConnString = String.Format("Data Source={0};Initial Catalog={1};Integrated Security=False;User ID={2};Password={3};Pooling=true;Min Pool Size=1;Max Pool Size=5;Connect Timeout=500", Server, Database, User, Password);
            DokanNet.DokanMain(opt, new MSSQLFS(ConnString));
        }
    }
}

/*

     System.Configuration.Configuration config =ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);

            config.AppSettings.Settings["oldPlace"].Value = "3";     
            config.Save(ConfigurationSaveMode.Modified);
            ConfigurationManager.RefreshSection("appSettings");
*/