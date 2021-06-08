package test.activej.fs;

import io.activej.config.Config;
import io.activej.inject.Injector;
import io.activej.launcher.Launcher;
import io.activej.launchers.fs.SimpleTcpServerLauncher;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * This example demonstrates configuring and launching ActiveFsServer.
 */
//[START EXAMPLE]
public class ServerSetupExample extends SimpleTcpServerLauncher {
	static {
		System.setProperty("chk:io.activej.bytebuf.ByteBuf", "on");
	}

	private Path storage;

	@Override
	protected void onInit(Injector injector) throws Exception {
		storage = Files.createTempDirectory("server_storage");
	}

	@Override
	protected Config createConfig() {
		return super.createConfig()
				.with("activefs.path", storage.toString())
				.with("activefs.listenAddresses", "*:6732");
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new ServerSetupExample();
		launcher.launch(args);
	}
}
//[END EXAMPLE]
