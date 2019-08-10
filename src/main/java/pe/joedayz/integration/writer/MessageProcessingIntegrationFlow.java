package pe.joedayz.integration.writer;


import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.file.FileNameGenerator;
import org.springframework.integration.file.FileWritingMessageHandler;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.messaging.MessageHandler;
import org.springframework.stereotype.Component;

import pe.joedayz.integration.configuration.ApplicationConfiguration;

@Component
public class MessageProcessingIntegrationFlow {

    public static final String CABECERA_FILENAME_GENERATOR = "cabeceraFilenameGenerator";
    public static final String DETALLE_FILENAME_GENERATOR = "detalleFilenameGenerator";
    public static final String LEYENDA_FILENAME_GENERATOR = "leyendaFilenameGenerator";
    public static final String CABECERA_MESSAGE_HANDLER = "cabeceraMessageHandler";
    public static final String DETALLE_MESSAGE_HANDLER = "detalleMessageHandler";
    public static final String LEYENDA_MESSAGE_HANDLER = "leyendaMessageHandler";
    
    @Autowired
    public File inboundOutDirectory;


    /**
     * Reverse the contents of the string and write it out using a filename generator to name the file
     *
     * @param fileWritingMessageHandler
     * @return
     */
    @Bean
    public IntegrationFlow saveCabecera(@Qualifier("cabeceraMessageHandler") MessageHandler fileWritingMessageHandler) {
        return IntegrationFlows.from(ApplicationConfiguration.CABECERA_CHANNEL)
        		.transform(m -> saveCabecera(m))
                .handle(fileWritingMessageHandler)
                .handle(loggingHandler())
                .get();
    }

    
    @Bean
    public IntegrationFlow saveDetalle(@Qualifier("detalleMessageHandler") MessageHandler fileWritingMessageHandler) {
        return IntegrationFlows.from(ApplicationConfiguration.DETALLE_CHANNEL)
        		.transform(m -> saveDetalle(m))
                .handle(fileWritingMessageHandler)
                .handle(loggingHandler())
                .get();
    }
    
    
    @Bean
    public IntegrationFlow saveLeyenda(@Qualifier("leyendaMessageHandler") MessageHandler fileWritingMessageHandler) {
        return IntegrationFlows.from(ApplicationConfiguration.LEYENDA_CHANNEL)
        		.transform(m -> saveLeyenda(m))
                .handle(fileWritingMessageHandler)
                .handle(loggingHandler())
                .get();
    }
    

    private String saveCabecera(Object m) {
		
    	String rpta = (String)m;
    	
    	System.out.println("Cabecera = " + rpta);
    	
		return rpta;
	}
    
    private String saveDetalle(Object m) {
		
    	String rpta = (String)m;
    	
    	System.out.println("Detalle = " + rpta);
    	
    	
		return rpta;
	}
    
    private String saveLeyenda(Object m) {
		
    	String rpta = (String)m;
    	
    	System.out.println("Leyenda = " + rpta);
    	
    	
		return rpta;
	}


	@Bean (name = CABECERA_MESSAGE_HANDLER)
    public MessageHandler cabeceraMessageHandler(@Qualifier(CABECERA_FILENAME_GENERATOR) FileNameGenerator fileNameGenerator) {
        FileWritingMessageHandler handler = new FileWritingMessageHandler(inboundOutDirectory);
        handler.setAutoCreateDirectory(true);
        handler.setFileNameGenerator(fileNameGenerator);
        return handler;
    }

	@Bean (name = DETALLE_MESSAGE_HANDLER)
    public MessageHandler detalleMessageHandler(@Qualifier(DETALLE_FILENAME_GENERATOR) FileNameGenerator fileNameGenerator) {
        FileWritingMessageHandler handler = new FileWritingMessageHandler(inboundOutDirectory);
        handler.setAutoCreateDirectory(true);
        handler.setFileNameGenerator(fileNameGenerator);
        return handler;
    }
	
	
	@Bean (name = LEYENDA_MESSAGE_HANDLER)
    public MessageHandler leyendaMessageHandler(@Qualifier(LEYENDA_FILENAME_GENERATOR) FileNameGenerator fileNameGenerator) {
        FileWritingMessageHandler handler = new FileWritingMessageHandler(inboundOutDirectory);
        handler.setAutoCreateDirectory(true);
        handler.setFileNameGenerator(fileNameGenerator);
        return handler;
    }
	
	
    @Bean
    public MessageHandler loggingHandler() {
        LoggingHandler logger = new LoggingHandler("INFO");
        logger.setShouldLogFullMessage(true);
        return logger;
    }

    @Bean(name = CABECERA_FILENAME_GENERATOR)
    public FileNameGenerator outboundCabecera(@Value("${out.filename.dateFormat}") String dateFormat, @Value("${out.filename.suffix}") String filenameSuffix) {
        return message -> "cabecera_" + DateTimeFormatter.ofPattern(dateFormat).format(LocalDateTime.now()) + filenameSuffix;
    }
    
    @Bean(name = DETALLE_FILENAME_GENERATOR)
    public FileNameGenerator outboundDetalle(@Value("${out.filename.dateFormat}") String dateFormat, @Value("${out.filename.suffix}") String filenameSuffix) {
        return message -> "detalle_" + DateTimeFormatter.ofPattern(dateFormat).format(LocalDateTime.now()) + filenameSuffix;
    }
    
    
    @Bean(name = LEYENDA_FILENAME_GENERATOR)
    public FileNameGenerator outboundLeyenda(@Value("${out.filename.dateFormat}") String dateFormat, @Value("${out.filename.suffix}") String filenameSuffix) {
        return message -> "leyenda_" + DateTimeFormatter.ofPattern(dateFormat).format(LocalDateTime.now()) + filenameSuffix;
    }

}
