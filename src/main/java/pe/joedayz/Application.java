package pe.joedayz;


import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

/**
 *
 * @author JoeDayz
 */
@SpringBootApplication
public class Application {

    public static void main(String[] args) {
     
    	new SpringApplicationBuilder(Application.class)
        .web(WebApplicationType.NONE) // .REACTIVE, .SERVLET
        .run(args);
    }

}






