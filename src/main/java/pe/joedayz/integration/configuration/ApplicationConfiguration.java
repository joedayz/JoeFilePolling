package pe.joedayz.integration.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.messaging.MessageChannel;

/**
 * Global Channel definitions
 *
 * @author JoeDayz
 */
@Configuration
public class ApplicationConfiguration {

    public static final String CABECERA_CHANNEL = "cabecera-channel";
    public static final String DETALLE_CHANNEL = "detalle-channel";
    public static final String LEYENDA_CHANNEL = "leyenda-channel";


    @Bean(name = CABECERA_CHANNEL)
    public MessageChannel cabeceraFilePollingChannel() {
        return MessageChannels.direct().get();
    }

    @Bean(name = DETALLE_CHANNEL)
    public MessageChannel detalleFilePollingChannel() {
        return MessageChannels.direct().get();
    }
    
    @Bean(name = LEYENDA_CHANNEL)
    public MessageChannel leyendaFilePollingChannel() {
        return MessageChannels.direct().get();
    }
    
}
