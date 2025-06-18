package es.ulpgc.client;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import com.formdev.flatlaf.FlatIntelliJLaf;
import es.ulpgc.data.CSVDataSource;
import es.ulpgc.data.DataSource;
import es.ulpgc.data.DatamartDataSource;
import es.ulpgc.service.*;

public class GUIHandler extends JFrame {
    private final JComboBox<String> dataSourceSelector;
    private final JButton loadButton;
    private final JTextField queryField;
    private final JTextField titleFilterField;
    private final JTextField authorFilterField;
    private final JTextField dateFilterField;
    private final JTextField languageFilterField;
    private final JTextField creditsFilterField;
    private final JButton searchButton;
    private final JTextArea resultsArea;
    private InvertedIndex invertedIndex;

    public GUIHandler() {
        try {
            UIManager.setLookAndFeel(new FlatIntelliJLaf());
        } catch (UnsupportedLookAndFeelException e) {
            e.printStackTrace();
        }
        setTitle("Search Engine");
        setSize(800, 600);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLayout(new BorderLayout());

        JPanel topPanel = new JPanel();
        topPanel.setLayout(new FlowLayout());

        JLabel dataSourceLabel = new JLabel("1. Choose your data source:");
        dataSourceSelector = new JComboBox<>(new String[]{"CSV", "Datamart"});
        loadButton = new JButton("Load");

        topPanel.add(dataSourceLabel);
        topPanel.add(dataSourceSelector);
        topPanel.add(loadButton);

        JPanel centerPanel = new JPanel();
        centerPanel.setLayout(new BorderLayout());

        JPanel queryPanel = new JPanel();
        queryPanel.setLayout(new GridLayout(6, 2, 5, 5)); // Grid layout for fields and filters

        JLabel queryLabel = new JLabel("Query:");
        queryField = new JTextField(30);

        JLabel titleFilterLabel = new JLabel("Filter by Title:");
        titleFilterField = new JTextField(30);

        JLabel authorFilterLabel = new JLabel("Filter by Author:");
        authorFilterField = new JTextField(30);

        JLabel dateFilterLabel = new JLabel("Filter by Date:");
        dateFilterField = new JTextField(30);

        JLabel languageFilterLabel = new JLabel("Filter by Language:");
        languageFilterField = new JTextField(30);

        JLabel creditsFilterLabel = new JLabel("Filter by Credits:");
        creditsFilterField = new JTextField(30);

        queryPanel.add(queryLabel);
        queryPanel.add(queryField);
        queryPanel.add(titleFilterLabel);
        queryPanel.add(titleFilterField);
        queryPanel.add(authorFilterLabel);
        queryPanel.add(authorFilterField);
        queryPanel.add(dateFilterLabel);
        queryPanel.add(dateFilterField);
        queryPanel.add(languageFilterLabel);
        queryPanel.add(languageFilterField);
        queryPanel.add(creditsFilterLabel);
        queryPanel.add(creditsFilterField);

        searchButton = new JButton("Search");
        resultsArea = new JTextArea();
        resultsArea.setEditable(false);
        JScrollPane scrollPane = new JScrollPane(resultsArea);

        centerPanel.add(queryPanel, BorderLayout.NORTH);
        centerPanel.add(scrollPane, BorderLayout.CENTER);

        // Add panels to the main frame
        add(topPanel, BorderLayout.NORTH);
        add(centerPanel, BorderLayout.CENTER);
        add(searchButton, BorderLayout.SOUTH);

        // Configure events
        configureEvents();
    }

    private void configureEvents() {
        // Event for loading the data source
        loadButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                String selectedSource = (String) dataSourceSelector.getSelectedItem();
                try {
                    assert selectedSource != null;
                    if (selectedSource.equals("CSV")) {
                        String filePath = "index_content.csv";
                        String metadataPath = "index_metadata.csv";
                        DataSource dataSource = new CSVDataSource(filePath, metadataPath);
                        invertedIndex = new InvertedIndex(dataSource);
                        JOptionPane.showMessageDialog(GUIHandler.this, "Data loaded from the CSV.");
                    } else if (selectedSource.equals("Datamart")) {
                        String datamartPath = "datamart_content";
                        String metadataPath = "datamart_metadata";
                        DataSource dataSource = new DatamartDataSource(datamartPath, metadataPath);
                        invertedIndex = new InvertedIndex(dataSource);
                        JOptionPane.showMessageDialog(GUIHandler.this, "Data loaded from the Datamart.");
                    }
                } catch (Exception ex) {
                    JOptionPane.showMessageDialog(GUIHandler.this, "Error loading the data source: " + ex.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
                }
            }
        });

        // Event for performing the search
        searchButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                if (invertedIndex == null) {
                    JOptionPane.showMessageDialog(GUIHandler.this, "You must first load a data source.", "Warning", JOptionPane.WARNING_MESSAGE);
                    return;
                }

                String query = queryField.getText().trim();
                if (query.isEmpty()) {
                    JOptionPane.showMessageDialog(GUIHandler.this, "Write a query to search.", "Warning", JOptionPane.WARNING_MESSAGE);
                    return;
                }

                QueryTokenizer tokenizer = new QueryTokenizer();
                Set<String> finalResults = null;

                try {
                    // Tokenize and search for each term in the query
                    for (String token : tokenizer.tokenize(new Query(query))) {
                        Set<String> results = invertedIndex.search(token);
                        if (finalResults == null) {
                            finalResults = results;
                        } else {
                            finalResults.retainAll(results);
                        }
                    }

                    Map<String, String> filters = new HashMap<>();
                    if (!titleFilterField.getText().trim().isEmpty()) {
                        filters.put("Title", titleFilterField.getText().trim().toLowerCase());
                    }
                    if (!authorFilterField.getText().trim().isEmpty()) {
                        filters.put("Author", authorFilterField.getText().trim().toLowerCase());
                    }
                    if (!dateFilterField.getText().trim().isEmpty()) {
                        filters.put("Date", dateFilterField.getText().trim().toLowerCase());
                    }
                    if (!languageFilterField.getText().trim().isEmpty()) {
                        filters.put("Language", languageFilterField.getText().trim().toLowerCase());
                    }
                    if (!creditsFilterField.getText().trim().isEmpty()) {
                        filters.put("Credits", creditsFilterField.getText().trim().toLowerCase());
                    }

                    // Apply filters if any
                    if (!filters.isEmpty() && finalResults != null) {
                        finalResults = invertedIndex.filterByMultipleMetadata(finalResults, filters);
                    }

                    // Display results
                    if (finalResults != null && !finalResults.isEmpty()) {
                        resultsArea.setText("Indexes found for the query \"" + query + "\":\n");
                        for (String result : finalResults) {
                            resultsArea.append(result + "\n");
                        }
                    } else {
                        resultsArea.setText("No results were found for the query \"" + query + "\".");
                    }
                } catch (Exception ex) {
                    resultsArea.setText("An error occurred during the search: " + ex.getMessage());
                }
            }
        });
    }
}
