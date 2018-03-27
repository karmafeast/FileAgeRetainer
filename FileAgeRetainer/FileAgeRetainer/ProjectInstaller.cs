using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration.Install;
using System.Linq;
using System.Threading.Tasks;

namespace FileAgeRetainer
{
    /// <summary>
    /// project installer for the FileAgeRetainer
    /// </summary>
    [RunInstaller(true)]
    public partial class ProjectInstaller : System.Configuration.Install.Installer
    {
        /// <summary>
        /// instantiator for project installer
        /// </summary>
        public ProjectInstaller()
        {
            InitializeComponent();
        }
    }
}
