package com.gpb.datafirewall.kafka;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.gpb.datafirewall.kafka.cef.SvoiLogger;
import com.gpb.datafirewall.kafka.cef.enums.SvoiSeverityEnum;
import com.gpb.datafirewall.kafka.cef.model.Log;
import com.gpb.datafirewall.kafka.cef.repository.LogPartitionRepository;
import com.gpb.datafirewall.kafka.cef.repository.LogRepository;
import com.gpb.datafirewall.kafka.utils.Utils;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@EnableJpaRepositories
@SpringBootApplication
@EnableScheduling
public class KafkaApplication {

	private final SvoiLogger svoiCustomLogger;
	private final LogPartitionRepository logPartitionRepository;
	private final LogRepository logRepository;
	private final ConfigurableEnvironment configurableEnvironment;
	private static ConfigurableApplicationContext applicationContext;

	@PostConstruct
	public void startupApplication() {
		logPartitionRepository.createTodayPartition();
		svoiCustomLogger.sendInternal(
				"startService", 
				"Start Service", 
				"Started service", 
				SvoiSeverityEnum.ONE);
		
		checkConfigChanges();
	}

	private void checkConfigChanges() {
		String props = Utils.getSources(configurableEnvironment.getPropertySources());
		String propsHash = Utils.getHash(props, "SHA-256");
		String localHostName = getHostName();

		Log logEntity = logRepository.findLatestByType("checkConfig", localHostName);
		if (logEntity == null) {
			svoiCustomLogger.sendInternal(
					"checkConfig", 
					"Check Config", 
					propsHash, 
					SvoiSeverityEnum.ONE);
		} else {
			String prevHash = StringUtils.trim(
					StringUtils.substringBetween(logEntity.getLog(), "msg=", "deviceProcessName=")
			);
			if (!StringUtils.equals(prevHash, propsHash)) {
				svoiCustomLogger.sendInternal(
						"checkConfig", 
						"Check Config", 
						propsHash,
						 SvoiSeverityEnum.ONE);
			}
		}
	}

	private String getHostName() {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			return InetAddress.getLoopbackAddress().getHostName();
		}
	}

	public static void main(String[] args) {
		SpringApplication.run(KafkaApplication.class, args);
	}

	@PreDestroy
	public void shutdownApplication() {
		svoiCustomLogger.sendInternal(
				"stopService", 
				"Stop Service", 
				"Stopped service", 
				SvoiSeverityEnum.ONE);
	}

	public static void restart() {
		ApplicationArguments args = applicationContext.getBean(ApplicationArguments.class);
		Thread thread = new Thread(() -> {
			applicationContext.close();
			applicationContext = SpringApplication.run(KafkaApplication.class, args.getSourceArgs());
		});
		thread.setDaemon(false);
		thread.start();
	}

}
