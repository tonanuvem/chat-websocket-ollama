package fiap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnError;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.server.PathParam;
import jakarta.websocket.server.ServerEndpoint;
import jakarta.websocket.Session;

// NOVOS IMPORTS PARA OLLAMA
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.atomic.AtomicBoolean;
import java.time.Duration;

@ServerEndpoint("/chat/{username}")
@ApplicationScoped
public class StartWebSocket {

    Map<String, Session> sessions = new ConcurrentHashMap<>();

    @OnOpen
    public void onOpen(Session session, @PathParam("username") String username) {
        sessions.put(username, session);
        broadcast("Usuário '" + username + "' entrou\n");
    }

    @OnClose
    public void onClose(Session session, @PathParam("username") String username) {
        sessions.remove(username);
        broadcast("Usuário :" + username + " : saiu\n");
    }

    @OnError
    public void onError(Session session, @PathParam("username") String username, Throwable throwable) {
        sessions.remove(username);
        broadcast("Usuário :" + username + " : saiu com erro: " + throwable + "\n");
    }

    @OnMessage
    public void onMessage(String message, @PathParam("username") String username) {
        // 1. Mostra o que o usuário digitou e quebra a linha
        broadcast(">> " + username + ": " + message + "\n");
        
        // 2. Prepara o prefixo do bot informando que está pensando
        broadcast(">> Ollama: pensando ");
        
        // 3. Dispara a chamada ao modelo de forma assíncrona
        chamarOllamaStreaming(message);
    }

    public void broadcast(String message) {
        sessions.values().forEach(s -> {
            s.getAsyncRemote().sendObject(message, result ->  {
                if (result.getException() != null) {
                    System.out.println("Unable to send message: " + result.getException());
                }
            });
        });
    }

    // NOVO METODO INTEGRAR LLM LOCAL: OLLAMA
    private void chamarOllamaStreaming(String prompt) {
        // Flag de controle compartilhada entre as threads
        AtomicBoolean primeiroToken = new AtomicBoolean(true);

        // --- THREAD 1: O Cronômetro Visual (Pontinhos a cada 1 seg) ---
        CompletableFuture.runAsync(() -> {
            int segundos = 0;
            // Limite de segurança de 60 segundos
            while (primeiroToken.get() && segundos < 60) {
                try {
                    Thread.sleep(1000); // Aguarda 1 segundo
                } catch (InterruptedException e) { }
                
                // Checa novamente antes de imprimir
                if (primeiroToken.get()) {
                    broadcast(".");
                    segundos++;
                }
            }
            // Se chegou a 60s e o bot ainda não respondeu
            if (primeiroToken.get()) {
                broadcast(" [Timeout: O modelo demorou muito para responder]\n");
            }
        });

        // --- THREAD 2: A Chamada HTTP com Timeout para o Ollama ---
        CompletableFuture.runAsync(() -> {
            try {
                // Instancia localmente para garantir isolamento e thread-safety
                ObjectMapper mapper = new ObjectMapper();
                String jsonBody = mapper.writeValueAsString(
                    Map.of("model", "gemma2:2b", "prompt", prompt, "stream", true)
                );

                HttpClient client = HttpClient.newHttpClient();
                HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:11434/api/generate"))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(60)) // Aborta após 60 segundos
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                    .build();

                // Processamento reativo lendo linha por linha
                client.sendAsync(request, HttpResponse.BodyHandlers.ofLines())
                    .thenAccept(response -> {
                        Stream<String> lines = response.body();
                        lines.forEach(line -> {
                            try {
                                JsonNode node = mapper.readTree(line);
                                String token = node.get("response").asText();
                                
                                // Recebeu primeiro token resposta: paralisa pontinhos
                                if (primeiroToken.get()) {
                                    primeiroToken.set(false); 
                                    broadcast(" Resposta: "); 
                                }
                                
                                broadcast(token);
                            } catch (Exception e) {
                                // Ignora interrupções vazias de parsing
                            }
                        });
                        broadcast("\n"); // Pula de linha ao terminar a resposta final
                    }).exceptionally(ex -> {
                        // Trata erros de rede ou estouro de timeout
                        if (primeiroToken.get()) {
                            primeiroToken.set(false);
                            broadcast(" [Erro: Falha na comunicação ou tempo limite esgotado]\n");
                        }
                        return null;
                    });
            } catch (Exception e) {
                primeiroToken.set(false);
                e.printStackTrace();
            }
        });
    }
}
