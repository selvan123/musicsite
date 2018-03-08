using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using BusinessLogicLayer;

namespace MusicSiteTest
{
    [TestClass]
    public class Index
    {
        [TestMethod]
        public void bindTopDownloads()
        {
            clsCommonHelper objcommon;
            Index index = new Index();
            objcommon = new clsCommonHelper();
            Assert.IsNotNull(objcommon);
        }
    }
}
