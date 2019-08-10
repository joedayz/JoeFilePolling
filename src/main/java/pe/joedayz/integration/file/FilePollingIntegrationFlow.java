package pe.joedayz.integration.file;

import java.io.File;
import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.core.task.TaskExecutor;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.file.DirectoryScanner;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.RecursiveDirectoryScanner;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.file.filters.AcceptOnceFileListFilter;
import org.springframework.integration.file.filters.CompositeFileListFilter;
import org.springframework.integration.file.filters.RegexPatternFileListFilter;
import org.springframework.integration.transaction.DefaultTransactionSynchronizationFactory;
import org.springframework.integration.transaction.ExpressionEvaluatingTransactionSynchronizationProcessor;
import org.springframework.integration.transaction.PseudoTransactionManager;
import org.springframework.integration.transaction.TransactionSynchronizationFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import pe.joedayz.integration.configuration.ApplicationConfiguration;

/**
 *  Inbound File Adapter looks for files that match the given regular expression
 *  Any files that have already been processed with the same name within the same
 *  JVM session will be ignored
 *  The poller is transactional and will move the file to a processed directory on successful
 *  downstream processing. If there is an exception in processing the file will be moved to a
 *  failed directory
 *
 *  @author JoeDayz
 */
@Configuration
class FilePollingIntegrationFlow {

    @Autowired
    public File inboundReadDirectory;

    @Autowired
    private ApplicationContext applicationContext;

    @Bean
    public IntegrationFlow cabeceraFileIntegration(@Value("${inbound.file.poller.fixed.delay}") long period,
                                                  @Value("${inbound.file.poller.max.messages.per.poll}") int maxMessagesPerPoll,
                                                  TaskExecutor taskExecutor,
                                                  @Qualifier("CABECERA_MESSAGE_SOURCE") MessageSource<File> fileReadingCabeceraMessageSource) {
        return IntegrationFlows.from(fileReadingCabeceraMessageSource,
                c -> c.poller(Pollers.fixedDelay(period)
                        .taskExecutor(taskExecutor)
                        .maxMessagesPerPoll(maxMessagesPerPoll)
                        .transactionSynchronizationFactory(transactionSynchronizationFactory())
                        .transactional(transactionManager())))
                .transform(Files.toStringTransformer())
                .channel(ApplicationConfiguration.CABECERA_CHANNEL)
                .get();
    }
    
    
    @Bean
    public IntegrationFlow detalleFileIntegration(@Value("${inbound.file.poller.fixed.delay}") long period,
                                                  @Value("${inbound.file.poller.max.messages.per.poll}") int maxMessagesPerPoll,
                                                  TaskExecutor taskExecutor,
                                                  @Qualifier("DETALLE_MESSAGE_SOURCE") MessageSource<File> fileReadingDetalleMessageSource) {
        return IntegrationFlows.from(fileReadingDetalleMessageSource,
                c -> c.poller(Pollers.fixedDelay(period)
                        .taskExecutor(taskExecutor)
                        .maxMessagesPerPoll(maxMessagesPerPoll)
                        .transactionSynchronizationFactory(transactionSynchronizationFactory())
                        .transactional(transactionManager())))
                .transform(Files.toStringTransformer())
                .channel(ApplicationConfiguration.DETALLE_CHANNEL)
                .get();
    }
    
    
    @Bean
    public IntegrationFlow leyendaFileIntegration(@Value("${inbound.file.poller.fixed.delay}") long period,
                                                  @Value("${inbound.file.poller.max.messages.per.poll}") int maxMessagesPerPoll,
                                                  TaskExecutor taskExecutor,
                                                  @Qualifier("LEYENDA_MESSAGE_SOURCE")  MessageSource<File> fileReadingLeyendaMessageSource) {
        return IntegrationFlows.from(fileReadingLeyendaMessageSource,
                c -> c.poller(Pollers.fixedDelay(period)
                        .taskExecutor(taskExecutor)
                        .maxMessagesPerPoll(maxMessagesPerPoll)
                        .transactionSynchronizationFactory(transactionSynchronizationFactory())
                        .transactional(transactionManager())))
                .transform(Files.toStringTransformer())
                .channel(ApplicationConfiguration.LEYENDA_CHANNEL)
                .get();
    }
    

    @Bean
    TaskExecutor taskExecutor(@Value("${inbound.file.poller.thread.pool.size}") int poolSize) {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(poolSize);
        return taskExecutor;
    }

    @Bean
    PseudoTransactionManager transactionManager() {
        return new PseudoTransactionManager();
    }

    @Bean
    TransactionSynchronizationFactory transactionSynchronizationFactory() {
        ExpressionParser parser = new SpelExpressionParser();
        ExpressionEvaluatingTransactionSynchronizationProcessor syncProcessor =
                new ExpressionEvaluatingTransactionSynchronizationProcessor();
        syncProcessor.setBeanFactory(applicationContext.getAutowireCapableBeanFactory());
        syncProcessor.setAfterCommitExpression(parser.parseExpression("payload.renameTo(new java.io.File(@inboundProcessedDirectory.path " +
                " + T(java.io.File).separator + payload.name))"));
        syncProcessor.setAfterRollbackExpression(parser.parseExpression("payload.renameTo(new java.io.File(@inboundFailedDirectory.path " +
                " + T(java.io.File).separator + payload.name))"));
        return new DefaultTransactionSynchronizationFactory(syncProcessor);
    }

    @Bean(name="CABECERA_MESSAGE_SOURCE")
    public FileReadingMessageSource fileReadingCabeceraMessageSource(@Qualifier("CABECERA_SCANNER") DirectoryScanner directoryScanner) {
        FileReadingMessageSource source = new FileReadingMessageSource();
        source.setDirectory(this.inboundReadDirectory);
        source.setScanner(directoryScanner);
        source.setAutoCreateDirectory(true);
        return source;
    }
    
    @Bean(name="DETALLE_MESSAGE_SOURCE")
    public FileReadingMessageSource fileReadingDetalleMessageSource(@Qualifier("DETALLE_SCANNER") DirectoryScanner directoryScanner) {
        FileReadingMessageSource source = new FileReadingMessageSource();
        source.setDirectory(this.inboundReadDirectory);
        source.setScanner(directoryScanner);
        source.setAutoCreateDirectory(true);
        return source;
    }
    
    
    @Bean(name="LEYENDA_MESSAGE_SOURCE")
    public FileReadingMessageSource fileReadingLeyendaMessageSource(@Qualifier("LEYENDA_SCANNER") DirectoryScanner directoryScanner) {
        FileReadingMessageSource source = new FileReadingMessageSource();
        source.setDirectory(this.inboundReadDirectory);
        source.setScanner(directoryScanner);
        source.setAutoCreateDirectory(true);
        return source;
    }
    

    @Bean(name="CABECERA_SCANNER")
    public DirectoryScanner directoryCabeceraScanner(@Value("${inbound.cabecera.regex}") String regex) {
        DirectoryScanner scanner = new RecursiveDirectoryScanner();
        CompositeFileListFilter<File> filter = new CompositeFileListFilter<>(
                Arrays.asList(new AcceptOnceFileListFilter<>(),
                        new RegexPatternFileListFilter(regex))
        );
        scanner.setFilter(filter);
        return scanner;
    }

    @Bean(name="DETALLE_SCANNER")
    public DirectoryScanner directoryDetalleScanner(@Value("${inbound.detalle.regex}") String regex) {
        DirectoryScanner scanner = new RecursiveDirectoryScanner();
        CompositeFileListFilter<File> filter = new CompositeFileListFilter<>(
                Arrays.asList(new AcceptOnceFileListFilter<>(),
                        new RegexPatternFileListFilter(regex))
        );
        scanner.setFilter(filter);
        return scanner;
    }
    
    @Bean(name="LEYENDA_SCANNER")
    public DirectoryScanner directoryLeyendaScanner(@Value("${inbound.leyenda.regex}") String regex) {
        DirectoryScanner scanner = new RecursiveDirectoryScanner();
        CompositeFileListFilter<File> filter = new CompositeFileListFilter<>(
                Arrays.asList(new AcceptOnceFileListFilter<>(),
                        new RegexPatternFileListFilter(regex))
        );
        scanner.setFilter(filter);
        return scanner;
    }

}