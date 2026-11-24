package serverAtonducanVillanueva;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

// STUDY CIRCLE SERVER - Simple Version
// Job: Accept clients, share messages, handle Q&A and Polls
public class ServerAtonducanVillanueva {

    // ===== 1. DATA WE KEEP =====
    // nick -> writer to send messages to that client
    private ConcurrentHashMap<String, PrintWriter> clients = new ConcurrentHashMap<>();

    // Questions: id -> question text. Example: 1 -> "What is Java? (by aven)"
    private ConcurrentHashMap<Integer, String> questions = new ConcurrentHashMap<>();
    // id -> true if answered, false if still open
    private ConcurrentHashMap<Integer, Boolean> isAnswered = new ConcurrentHashMap<>();
    private int nextQuestionId = 1;

    // Poll data
    private String pollQuestion = null;
    private List<String> pollOptions = new ArrayList<>();
    private List<Integer> pollVotes = new ArrayList<>();
    private Set<String> alreadyVoted = ConcurrentHashMap.newKeySet();

    // ===== 2. START SERVER =====
    public static void main(String[] args) throws Exception {
        int port = 5000; // fixed port, easy to remember
        new ServerAtonducanVillanueva().startServer(port);
    }

    void startServer(int port) throws Exception {
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("Study Circle Server running on port " + port);
        ExecutorService pool = Executors.newCachedThreadPool();

        // Forever: wait for a new client, give them their own Handler
        while (true) {
            Socket clientSocket = serverSocket.accept();
            pool.execute(new ClientHandler(clientSocket));
        }
    }

    // ===== 3. HELPERS TO SEND MESSAGES =====

    // Send to EVERYONE
    void sendToAll(String message) {
        for (PrintWriter writer : clients.values()) {
            synchronized (writer) {
                writer.println(message);
            }
        }
    }

    // Send updated user list to everyone
    void sendUserList() {
        sendToAll("USERS:" + String.join(",", new TreeSet<>(clients.keySet())));
    }

    // Make the QUESTIONS text: "1. [OPEN] What is Java? ;; 2. [ANSWERED] ..."
    String makeQuestionList() {
        if (questions.isEmpty()) {
            return "No questions yet. Use /ask <question>";
        }
        StringBuilder result = new StringBuilder();
        for (int id : new TreeSet<>(questions.keySet())) {
            String status = isAnswered.get(id) ? "[ANSWERED] " : "[OPEN] ";
            result.append(id).append(". ").append(status).append(questions.get(id)).append(" ;; ");
        }
        return result.toString();
    }

    // Make the POLL text: "POLL: Q ;; 1. A - 0 votes ;; Total: 0 | /vote <num>"
    String makePollText() {
        if (pollQuestion == null) {
            return "No active poll. Use /poll Q | A | B";
        }
        StringBuilder result = new StringBuilder("POLL: " + pollQuestion + " ;; ");
        int total = 0;
        for (int votes : pollVotes) total += votes;

        for (int i = 0; i < pollOptions.size(); i++) {
            result.append(i + 1).append(". ").append(pollOptions.get(i))
                  .append(" - ").append(pollVotes.get(i)).append(" votes ;; ");
        }
        return result.append("Total: ").append(total).append(" | /vote <num>").toString();
    }

    // ===== 4. ONE CLIENT = ONE HANDLER =====
    class ClientHandler implements Runnable {
        Socket socket;
        PrintWriter out; // to send to THIS client
        BufferedReader in; // to read from THIS client
        String nick;

        ClientHandler(Socket socket) {
            this.socket = socket;
        }

        public void run() {
            try {
                out = new PrintWriter(socket.getOutputStream(), true);
                in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                // First line from client must be nickname
                String name = in.readLine();
                if (name == null || name.trim().isEmpty() || clients.containsKey(name.trim())) {
                    out.println("ERROR:Nickname taken or blank.");
                    closeAll();
                    return;
                }

                nick = name.trim();
                clients.put(nick, out);
                out.println("WELCOME:" + nick);
                sendToAll("SYSTEM:" + nick + " joined the Study Circle.");
                sendUserList();
                out.println("QUESTIONS:" + makeQuestionList());
                out.println("POLL:" + makePollText());

                // Main loop: read 1 line, decide: command or chat?
                String line;
                while ((line = in.readLine()) != null) {
                    if (line.trim().isEmpty()) {
                        out.println("SYSTEM:Message cannot be blank.");
                        continue;
                    }

                    // Clean the message: Client sends "Juan: /ask ..." -> we need "/ask ..."
                    String clean = cleanMessage(line);

                    if (clean.startsWith("/")) {
                        handleCommand(clean); // it's a command
                    } else {
                        // it's normal chat
                        sendToAll("CHAT:[" + java.time.LocalTime.now().withNano(0) + "] " + nick + ": " + clean);
                    }
                }
            } catch (Exception e) {
                // client disconnected
            } finally {
                if (nick != null && clients.remove(nick) != null) {
                    sendToAll("SYSTEM:" + nick + " left.");
                    sendUserList();
                }
                closeAll();
            }
        }

        // "Juan: /ask What is Java?" -> "/ask What is Java?"
        // "Hello" -> "Hello"
        String cleanMessage(String line) {
            if (line.contains(": ")) {
                return line.substring(line.indexOf(": ") + 2);
            }
            return line;
        }

        // ===== 5. ALL COMMANDS - ONE METHOD EACH =====
        void handleCommand(String line) {
            // Split into: command + rest. Example: "/ask What is Java?" -> "/ask" + "What is Java?"
            String[] parts = line.split(" ", 2);
            String command = parts[0].toLowerCase();
            String argument = parts.length > 1 ? parts[1].trim() : "";

            if (command.equals("/help")) {
                out.println("SYSTEM:Commands: /help /name <new> /exit /ask <q> /answer <id> /questions /poll Q | A | B /vote <num> /endpoll");
            }
            else if (command.equals("/name")) {
                changeName(argument);
            }
            else if (command.equals("/exit")) {
                try { socket.close(); } catch (Exception e) {}
            }
            else if (command.equals("/ask")) {
                askQuestion(argument);
            }
            else if (command.equals("/answer")) {
                answerQuestion(argument);
            }
            else if (command.equals("/questions")) {
                out.println("QUESTIONS:" + makeQuestionList());
            }
            else if (command.equals("/poll")) {
                createPoll(argument);
            }
            else if (command.equals("/vote")) {
                vote(argument);
            }
            else if (command.equals("/endpoll")) {
                endPoll();
            }
            else {
                out.println("SYSTEM:Unknown command. Type /help");
            }
        }

        void changeName(String newName) {
            if (newName.isEmpty()) {
                out.println("SYSTEM:Usage: /name <newname>");
                return;
            }
            if (clients.containsKey(newName)) {
                out.println("SYSTEM:Name already taken.");
                return;
            }
            clients.remove(nick);
            String oldName = nick;
            nick = newName;
            clients.put(nick, out);
            sendToAll("SYSTEM:" + oldName + " is now " + nick);
            sendUserList();
        }

        void askQuestion(String question) {
            if (question.isEmpty()) {
                out.println("SYSTEM:Usage: /ask <question>");
                return;
            }
            int id = nextQuestionId++;
            questions.put(id, question + " (by " + nick + ")");
            isAnswered.put(id, false);
            sendToAll("QUESTIONS:" + makeQuestionList());
            sendToAll("SYSTEM:New question #" + id + " from " + nick);
        }

        void answerQuestion(String idText) {
            try {
                int id = Integer.parseInt(idText);
                if (!questions.containsKey(id)) {
                    out.println("SYSTEM:No such question.");
                    return;
                }
                isAnswered.put(id, true);
                sendToAll("QUESTIONS:" + makeQuestionList());
                sendToAll("SYSTEM:Question #" + id + " answered.");
            } catch (Exception e) {
                out.println("SYSTEM:Usage: /answer <id>");
            }
        }

        void createPoll(String text) {
            // Expected: "Favorite subject? | Math | English | Science"
            if (text.isEmpty() || !text.contains("|")) {
                out.println("SYSTEM:Usage: /poll Question | Option1 | Option2");
                return;
            }
            String[] pollParts = text.split("\\|");
            if (pollParts.length < 3) {
                out.println("SYSTEM:Need question + at least 2 options.");
                return;
            }
            pollQuestion = pollParts[0].trim();
            pollOptions = new ArrayList<>();
            pollVotes = new ArrayList<>();
            for (int i = 1; i < pollParts.length; i++) {
                pollOptions.add(pollParts[i].trim());
                pollVotes.add(0);
            }
            alreadyVoted.clear();
            sendToAll("POLL:" + makePollText());
        }

        void vote(String numberText) {
            if (pollQuestion == null) {
                out.println("SYSTEM:No active poll.");
                return;
            }
            if (!alreadyVoted.add(nick)) {
                out.println("SYSTEM:You already voted.");
                return;
            }
            try {
                int choice = Integer.parseInt(numberText);
                if (choice < 1 || choice > pollOptions.size()) {
                    alreadyVoted.remove(nick);
                    out.println("SYSTEM:Invalid vote number.");
                    return;
                }
                pollVotes.set(choice - 1, pollVotes.get(choice - 1) + 1);
                sendToAll("POLL:" + makePollText());
            } catch (Exception e) {
                alreadyVoted.remove(nick);
                out.println("SYSTEM:Usage: /vote <num>");
            }
        }

        void endPoll() {
            pollQuestion = null;
            pollOptions = new ArrayList<>();
            pollVotes = new ArrayList<>();
            alreadyVoted.clear();
            sendToAll("POLL:" + makePollText());
            sendToAll("SYSTEM:Poll ended by " + nick);
        }

        void closeAll() {
            try { if (in != null) in.close(); } catch (Exception e) {}
            try { if (out != null) out.close(); } catch (Exception e) {}
            try { if (socket != null) socket.close(); } catch (Exception e) {}
        }
    }
}

