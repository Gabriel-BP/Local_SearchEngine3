package es.ulpgc.entrypoint;

import es.ulpgc.client.GUIHandler;

import javax.swing.SwingUtilities;

public class QueryEngineGUI {
    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            GUIHandler gui = new GUIHandler();
            gui.setVisible(true);
        });
    }
}