%scala


def retryMount(source: String, mountPoint: String): Unit = {
    try {
        // Mount with IAM roles instead of keys for PVC
        dbutils.fs.mount(source, mountPoint)
    } catch {
        case e: Exception = > mountFailed(s"*** ERROR: Unable to mount $mountPoint: ${e.getMessage}")
    }
}


def mountFailed(msg: String): Unit = {
    println(msg)
}

# ========================================


%scala


def retryMount(source: String, mountPoint: String): Unit = {
    try {
        // Mount with IAM roles instead of keys for PVC
        dbutils.fs.mount(source, mountPoint)
    } catch {
        case e: Exception = > mountFailed(s"*** ERROR: Unable to mount $mountPoint: ${e.getMessage}")
    }
}


def mountFailed(msg: String): Unit = {
    println(msg)
}
# ========================================


%scala


def mountSource(mountDir: String, source: String, extraConfigs: Map[String, String]): String = {
    val mntSource = source

    if (dbutils.fs.mounts().map(_.mountPoint).contains(mountDir)) {
        val mount = dbutils.fs.mounts().filter(_.mountPoint == mountDir).head
        if (mount.source == mntSource) {
            return s"""Datasets are already mounted to <b>$mountDir</b> from <b>$mntSource</b>"""

        }
        else {
            return "Invalid Mounts"
        }
    }
    else {
        println(s"""Mounting datasets to $mountDir from $mntSource""")
        mount(source, extraConfigs, mountDir)
        return s"""Mounted datasets to <b>$mountDir</b> from <b>$mntSource<b>"""
    }
}


# ========================================
%scala


def getAzureRegion(): String = {
    import com.databricks.backend.common.util.Project
    import com.databricks.conf.trusted.ProjectConf
    import com.databricks.backend.daemon.driver.DriverConf

    new DriverConf(ProjectConf.loadLocalConfig(Project.Driver)).region
}

# ========================================


%scala


def initAzureDataSource(azureRegion: String): (String, Map[String, String]) = {
    var MAPPINGS = Map(
        "eastus" -> ("dbtraineastus",
                     "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=tlw5PMp1DMeyyBGTgZwTbA0IJjEm83TcCAu08jCnZUo%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"))



    val(account: String, sasKey: String) = MAPPINGS.getOrElse(azureRegion, MAPPINGS("_default"))
    val blob = "training"
    val source = s"wasbs://$blob@$account.blob.core.windows.net/"
    val config = Map(
        s"fs.azure.sas.$blob.$account.blob.core.windows.net" -> sasKey
    )
    val(sasEntity, sasToken) = config.head

    val datasource = "%s\t%s\t%s".format(source, sasEntity, sasToken)
    spark.conf.set("com.databricks.training.azure.datasource", datasource)

    return (source, config)
}


# ========================================
%scala


def autoMount(mountDir: String = "/mnt/training"): Unit = {
    val azureRegion = getAzureRegion()
    print(azureRegion+"\n")
    spark.conf.set("com.databricks.training.region.name", azureRegion)
    val(source, extraConfigs) = initAzureDataSource(azureRegion)
    val resultMsg = mountSource(mountDir, source, extraConfigs)
    displayHTML(resultMsg)
}

# ========================================


%scala
autoMount()
