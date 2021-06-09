package test.activej.fs;

import io.activej.csp.file.ChannelFileReader;
import io.activej.eventloop.Eventloop;
import io.activej.fs.tcp.RemoteActiveFs;
import io.activej.inject.Injector;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.service.ServiceGraphModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

/**
 * This example demonstrates uploading file to server using RemoteFS
 * To run this example you should first launch ServerSetupExample
 */
@SuppressWarnings("unused")
public final class FileUploadExample extends Launcher {
	static {
		System.setProperty("chk:io.activej.bytebuf.ByteBuf", "on");
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(FileUploadExample.class);

	private static final int SERVER_PORT = 6732;
	private static final String FILE_NAME = "example.txt";
	private static final String EXAMPLE_TEXT = "example text".repeat(10000);

	private Path clientFile;

	@Override
	protected void onInit(Injector injector) throws Exception {
		clientFile = Files.createTempFile("example", ".txt");
		Files.write(clientFile, EXAMPLE_TEXT.getBytes());
	}

	@Inject
	private RemoteActiveFs client;

	@Inject
	private Eventloop eventloop;

	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	RemoteActiveFs remoteFsClient(Eventloop eventloop) {
		return RemoteActiveFs.create(eventloop, new InetSocketAddress(SERVER_PORT));
	}

	@Override
	protected Module getModule() {
		return ServiceGraphModule.create();
	}

	//[START EXAMPLE]
	@Override
	protected void run() throws Exception {
		ExecutorService executor = newSingleThreadExecutor();
		CompletableFuture<Void> future = eventloop.submit(() ->
				// consumer result here is a marker of it being successfully uploaded
				ChannelFileReader.open(executor, clientFile)
						.whenResult(() -> LOGGER.info("File opened"))
						.then(cfr -> cfr
								.peek(buf -> LOGGER.info("FR emitted piece of data, size: {}", buf.readRemaining()))
								.withEndOfStream(voidPromise -> voidPromise.map($null -> {
									LOGGER.info("Got end of the stream in producer");
									return $null;
								}))
								.streamTo(client.upload(FILE_NAME, EXAMPLE_TEXT.length())
										.whenResult(() -> LOGGER.info("Consumer is opened"))
										.map(byteBufChannelConsumer -> byteBufChannelConsumer
												.peek(buf -> LOGGER.info("Consumer got piece of data, size: {}", buf.readRemaining()))
												.withAcknowledgement(ack -> ack.whenComplete(() -> LOGGER.info("Got ack")))))));
		try {
			future.get();
			LOGGER.info("Success");
		} finally {
			executor.shutdown();
		}
	}
	//[END EXAMPLE]

	public static void main(String[] args) throws Exception {
		FileUploadExample example = new FileUploadExample();
		example.launch(args);
	}
}
