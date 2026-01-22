// Global variables
let currentDate = new Date();
let globalData = [];
let satelliteRelationships = null; // Store relationship data
let autoLoadEnabled = false; // Flag to control automatic data loading
//const apiBasePath = 'cgi-bin'; 
const apiBasePath = 'assets/python'; 

$(document).ready(function(){
    updateCurrentDateDisplay();
    
    // Add confirm button container after filters
    addConfirmButtonToUI();
    
    // First fetch metadata (relationships)
    fetchMetadata(currentDate);
    
    // Toggle button event handler
    $("#toggleAutoload").on("change", function() {
        autoLoadEnabled = $(this).prop("checked");
        
        // Show or hide confirm button based on autoload setting
        if (autoLoadEnabled) {
            $("#confirmButtonContainer").hide();
        } else {
            $("#confirmButtonContainer").show();
        }
    });
    
    // Confirm button event handler
    $("#confirmButton").on("click", function() {
        // Show loading state on button
        $(this).html('<span class="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>Loading...');
        $(this).prop('disabled', true);
        
        // Fetch data
        fetchDataForDay(currentDate);
    });
});

// Add confirm button to the UI
function addConfirmButtonToUI() {
    console.log("Adding confirm button"); // Debug log
    
    // Try multiple selectors to find a suitable container
    // Using the instrument select itself if no container is found
    const $filtersContainer = $("#instrument_select").closest(".form-group").length ? 
                             $("#instrument_select").closest(".form-group") : 
                             $("#instrument_select").parent();
    
    console.log("Container found:", $filtersContainer.length > 0); // Debug log
    
    if ($filtersContainer.length) {
        // Add toggle switch for autoload - set unchecked to match autoLoadEnabled = false
        $filtersContainer.after(`
            <div class="form-group mt-3 mb-2">
                <div class="form-check form-switch">
                    <input class="form-check-input" type="checkbox" id="toggleAutoload">
                    <label class="form-check-label" for="toggleAutoload">Auto-load data when selections change</label>
                </div>
            </div>
            
            <div id="confirmButtonContainer" class="form-group mt-2 mb-3">
                <button id="confirmButton" class="btn btn-primary w-100">
                    <i class="bi bi-search me-2"></i>Load Data
                </button>
            </div>
        `);
        
        console.log("Button added to DOM"); // Debug log
    } else {
        // Fallback - add to body
        console.log("No container found, adding to body");
        $("body").append(`
            <div class="container mt-3">
                <div class="form-group mt-3 mb-2">
                    <div class="form-check form-switch">
                        <input class="form-check-input" type="checkbox" id="toggleAutoload">
                        <label class="form-check-label" for="toggleAutoload">Auto-load data when selections change</label>
                    </div>
                </div>
                
                <div id="confirmButtonContainer" class="form-group mt-2 mb-3">
                    <button id="confirmButton" class="btn btn-primary w-100">
                        <i class="bi bi-search me-2"></i>Load Data
                    </button>
                </div>
            </div>
        `);
    }
}

// Previous Day Button
$("#prevDay").on("click", function(){
    currentDate.setDate(currentDate.getDate() - 1);
    updateCurrentDateDisplay();
    // No need to fetch metadata again, we can reuse it
    fetchSatellitesForDay(currentDate);
    
    // Reset the confirm button if it's visible
    resetConfirmButton();
});

// Next Day Button
$("#nextDay").on("click", function(){
    currentDate.setDate(currentDate.getDate() + 1);
    updateCurrentDateDisplay();
    // No need to fetch metadata again, we can reuse it
    fetchSatellitesForDay(currentDate);
    
    // Reset the confirm button if it's visible
    resetConfirmButton();
});

// When satellite selection changes, update coverage options
$("#satellite_id").on("change", function(){
    updateCoverageFromRelationships();
    
    // Only fetch data if autoload is enabled
    if (autoLoadEnabled) {
        fetchDataForDay(currentDate);
    } else {
        resetConfirmButton();
    }
});

// When coverage selection changes, update instrument options
$("#coverage_select").on("change", function(){
    updateInstrumentFromRelationships();
    
    // Only fetch data if autoload is enabled
    if (autoLoadEnabled) {
        fetchDataForDay(currentDate);
    } else {
        resetConfirmButton();
    }
});

// When instrument selection changes
$("#instrument_select").on("change", function(){
    // Only fetch data if autoload is enabled
    if (autoLoadEnabled) {
        fetchDataForDay(currentDate);
    } else {
        resetConfirmButton();
    }
});

// Reset confirm button to its default state
function resetConfirmButton() {
    $("#confirmButton").html('<i class="bi bi-search me-2"></i>Load Data');
    $("#confirmButton").prop('disabled', false);
}

// Format and display the current date as YYYY-MM-DD
function updateCurrentDateDisplay(){
    let year = currentDate.getFullYear();
    let month = (currentDate.getMonth() + 1).toString().padStart(2, "0");
    let day = currentDate.getDate().toString().padStart(2, "0");
    $("#currentDateDisplay").text(`${year}-${month}-${day}`);
}

// Function 1: fetchMetadata - Complete updated function
function fetchMetadata(dateObj) {
    showLoading();
    
    $.ajax({
        url: apiBasePath + '/metadata.py',  
        method: 'GET',
        dataType: 'json',
        success: function(response) {
            console.log("Metadata loaded:", response);
            
            // Store the relationships globally
            satelliteRelationships = response;
            
            // Now fetch satellites
            fetchSatellitesForDay(dateObj);
        },
        error: function(jqXHR, textStatus, errorThrown) {
            hideLoading();
            console.error("Metadata API Error:", textStatus, errorThrown);
            showError("Error fetching metadata. Please try again later.");
            
            // Try to fetch satellites anyway
            fetchSatellitesForDay(dateObj);
        }
    });
}


// Update coverage dropdown based on selected satellite and relationships
function updateCoverageFromRelationships() {
    const selectedSatellite = $("#satellite_id").val();
    const $select = $("#coverage_select");
    const currentValue = $select.val();
    
    $select.empty();
    $select.append('<option value="">All Coverages</option>');
    
    if (!satelliteRelationships) {
        return; // No relationship data available
    }
    
    let coverages = [];
    
    if (selectedSatellite && satelliteRelationships.relationships[selectedSatellite]) {
        // Get coverages for the selected satellite
        coverages = satelliteRelationships.relationships[selectedSatellite].coverages;
    } else if (!selectedSatellite) {
        // If no satellite selected, show all coverages
        coverages = satelliteRelationships.coverages;
    }
    
    // Filter out "Not Available" for display unless it's the only option
    let filteredCoverages = coverages.filter(c => c && c !== "Not Available");
    if (filteredCoverages.length === 0 && coverages.includes("Not Available")) {
        filteredCoverages = ["Not Available"];
    }
    
    // Add options to dropdown
    filteredCoverages.sort().forEach(coverage => {
        $select.append(`<option value="${coverage}">${coverage}</option>`);
    });
    
    // Restore previous selection if it exists in new data
    if (currentValue && filteredCoverages.includes(currentValue)) {
        $select.val(currentValue);
    }
    
    // Update instruments based on the selected coverage
    updateInstrumentFromRelationships();
}

// Show/Hide loading indicators
function showLoading() {
    $("#loading").show();
    $("#errorMessage").hide();
    $("#chart").hide();
    $("#dataTable").hide();
}

function hideLoading() {
    $("#loading").hide();
    $("#chart").show();
    $("#dataTable").show();
}

function showError(message, details = "") {
    $("#errorMessage").html(`
        <strong>Error:</strong> ${message}
        ${details ? `<pre>${details}</pre>` : ""}
    `).show();
    $("#chart").hide();
    $("#dataTable").hide();
}

// Update debug info panel
function updateDebugInfo(data) {
    if (!data || data.length === 0) {
        $("#debugContent").html("<p>No data available</p>");
        return;
    }
    
    // Sample first data item
    const firstItem = data[0];
    const keys = Object.keys(firstItem);
    
    // Count of unique values for key fields
    const satelliteIDs = [...new Set(data.map(item => item.satellite_id || "undefined"))];
    const instruments = [...new Set(data.map(item => item.instrument || "undefined"))];
    const coverages = [...new Set(data.map(item => item.coverage || "undefined"))];
    
    let html = `
        <div class="mb-3">
            <strong>Data sample (first record):</strong>
            <pre>${JSON.stringify(firstItem, null, 2)}</pre>
        </div>
        <div class="mb-3">
            <strong>Available fields:</strong> ${keys.join(", ")}
        </div>
        <div class="mb-3">
            <strong>Unique satellite_id values (${satelliteIDs.length}):</strong> ${satelliteIDs.join(", ")}
        </div>
        <div class="mb-3">
            <strong>Unique instrument values (${instruments.length}):</strong> ${instruments.join(", ")}
        </div>
        <div class="mb-3">
            <strong>Unique coverage values (${coverages.length}):</strong> ${coverages.join(", ")}
        </div>
        <div class="mb-3">
            <strong>Total records:</strong> ${data.length}
        </div>
    `;
    
    $("#debugContent").html(html);
}

// Update satellite status display
function updateSatelliteStatus(satellites, baseDir) {
    const $status = $("#satelliteStatus");
    const $content = $("#satelliteStatusContent");
    const $dirPath = $("#directoryPath");
    
    $content.empty();
    satellites.forEach(sat => {
        const statusClass = sat.fileExists ? 'status-exists' : 'status-missing';
        const statusText = sat.fileExists ? 'File exists' : 'File missing';
        
        // Use displayName if available, otherwise use id
        const displayText = sat.displayName || sat.id;
        
        $content.append(`
            <div class="satellite-info">
                <span class="fw-bold">${displayText}:</span> 
                <span class="${statusClass}">${statusText}</span>
            </div>
        `);
    });
    
    $dirPath.html(`<strong>Directory:</strong> ${baseDir}`);
    $status.show();
}

// Update coverage dropdown based on available data
function updateCoverageDropdown(data) {
    const coverages = [...new Set(data.map(item => item.coverage))]
        .filter(c => c && c !== "Not Available")
        .sort();
    
    const $select = $("#coverage_select");
    const currentValue = $select.val();
    
    $select.empty();
    $select.append('<option value="">All Coverages</option>');
    
    coverages.forEach(coverage => {
        $select.append(`<option value="${coverage}">${coverage}</option>`);
    });
    
    // Restore previous selection if it exists in new data
    if (currentValue && coverages.includes(currentValue)) {
        $select.val(currentValue);
    }
}

// Update instrument dropdown based on available data
function updateInstrumentDropdown(data) {
    const instruments = [...new Set(data.map(item => item.instrument))]
        .filter(i => i && i !== "Not Available")
        .sort();
    
    const $select = $("#instrument_select");
    const currentValue = $select.val();
    
    $select.empty();
    $select.append('<option value="">All Instruments</option>');
    
    instruments.forEach(instrument => {
        $select.append(`<option value="${instrument}">${instrument}</option>`);
    });
    
    // Restore previous selection if it exists in new data
    if (currentValue && instruments.includes(currentValue)) {
        $select.val(currentValue);
    } else if (instruments.length > 0) {
        // Select first instrument by default if no previous selection
        $select.val(instruments[0]);
    }
}

// Optimized API response handling with memory management
function fetchDataForDay(dateObj){
    // Use UTC date methods instead of local time methods
    let year = dateObj.getUTCFullYear();
    let month = (dateObj.getUTCMonth() + 1).toString().padStart(2, "0");
    let day = dateObj.getUTCDate().toString().padStart(2, "0");
    let dateStr = `${year}-${month}-${day}`;
    
    // Get selected options
    let selectedSatellite = $("#satellite_id").val();
    let selectedCoverage = $("#coverage_select").val();
    let selectedInstrument = $("#instrument_select").val();
    
    // Use fixed interval (5T) - explicitly specify it's for UTC time
    let query = `start_date=${dateStr}&end_date=${dateStr}&start_hour=00:00&end_hour=23:59`;
    if (selectedSatellite) {
        query += `&satellite_id=${selectedSatellite}`;
    }
    if (selectedCoverage) {
        query += `&coverage=${selectedCoverage}`;
    }
    if (selectedInstrument) {
        query += `&instrument=${selectedInstrument}`;
    }
    
    console.log("Fetching data with query:", query);
    showLoading();
    
    // Cancel previous AJAX request if it exists
    if (window.currentAjaxRequest) {
        window.currentAjaxRequest.abort();
    }
    
    // Clear previous data to free memory
    if (window.globalData) {
        window.globalData = null;
    }
    
    // Use low-level XHR for better memory control
    window.currentAjaxRequest = $.ajax({
        url: apiBasePath + '/data.py?' + query,
        method: 'GET',
        dataType: 'json',
        success: function(response) {
            hideLoading();
            window.currentAjaxRequest = null;
            
            // Reset confirm button if it exists
            resetConfirmButton();
            
            try {
                if (response.data && response.data.length > 0) {
                    // Create UTC day boundaries for filtering
                    const currentUTCDay = new Date(Date.UTC(
                        dateObj.getUTCFullYear(),
                        dateObj.getUTCMonth(),
                        dateObj.getUTCDate()
                    ));
                    
                    const nextUTCDay = new Date(currentUTCDay);
                    nextUTCDay.setUTCDate(nextUTCDay.getUTCDate() + 1);
                    
                    // Pre-process data to ensure dates are properly parsed as UTC
                    response.data = response.data.map(item => {
                        if (item.start_time) {
                            item.start_time = new Date(item.start_time);
                        }
                        return item;
                    });
                    
                    // Filter data to only include entries from the selected UTC day
                    response.data = response.data.filter(item => 
                        item.start_time >= currentUTCDay && 
                        item.start_time < nextUTCDay
                    );
                    
                    console.log(`Filtered to ${response.data.length} records within UTC day: ${dateStr}`);
                    
                    // Store in global variable for reuse without refetching
                    window.globalData = response.data;
                    
                    // Update debug information
                    updateDebugInfo(response.data);
                    
                    // Update dropdowns
                    updateCoverageDropdown(response.data);
                    updateInstrumentDropdown(response.data);
                    
                    // Apply filters - Use efficient filtering
                    let filteredData;
                    
                    if (selectedCoverage || selectedInstrument) {
                        filteredData = response.data.filter(item => {
                            let matchesCoverage = !selectedCoverage || item.coverage === selectedCoverage;
                            let matchesInstrument = !selectedInstrument || item.instrument === selectedInstrument;
                            return matchesCoverage && matchesInstrument;
                        });
                    } else {
                        filteredData = response.data;
                    }
                    
                    if (filteredData.length > 0) {
                        // Pass UTC day boundaries to displayData
                        displayData(filteredData, currentUTCDay, nextUTCDay);
                    } else {
                        showError("No data available after applying filters.");
                    }
                } else {
                    showError("No data available for the selected period.");
                }
            } catch (e) {
                console.error("Error processing response:", e);
                showError("Error processing data: " + e.message, e.stack);
            }
        },
        error: function(jqXHR, textStatus, errorThrown) {
            hideLoading();
            window.currentAjaxRequest = null;
            
            // Reset confirm button if it exists
            resetConfirmButton();
            
            // Don't process if it was manually aborted
            if (textStatus === 'abort') {
                console.log("Request aborted");
                return;
            }
            
            const errorDetails = {
                url: this.url,
                status: jqXHR.status,
                statusText: jqXHR.statusText,
                responseText: jqXHR.responseText ? 
                    (jqXHR.responseText.length > 1000 ? 
                        jqXHR.responseText.substring(0, 1000) + '...' : 
                        jqXHR.responseText) : 
                    'No response text'
            };
            
            console.error("API Error:", errorDetails);
            
            const additionalDetails = {
                selectedSatellite: selectedSatellite,
                selectedCoverage: selectedCoverage,
                selectedInstrument: selectedInstrument,
                dateStr: dateStr
            };
            
            console.error("Error Details:", additionalDetails);
            
            // Display the error message with details on the website
            showError(
                "Error fetching data. Please see below for more details.",
                JSON.stringify(errorDetails, null, 2)
            );
        }
    });
}

// Update instrument dropdown based on selected satellite, coverage, and relationships
function updateInstrumentFromRelationships() {
    const selectedSatellite = $("#satellite_id").val();
    const selectedCoverage = $("#coverage_select").val();
    const $select = $("#instrument_select");
    const currentValue = $select.val();
    
    $select.empty();
    $select.append('<option value="">All Instruments</option>');
    
    if (!satelliteRelationships) {
        return; // No relationship data available
    }
    
    let instruments = [];
    
    if (selectedSatellite && satelliteRelationships.relationships[selectedSatellite]) {
        const satRel = satelliteRelationships.relationships[selectedSatellite];
        
        if (selectedCoverage && satRel.coverage_instruments && 
            satRel.coverage_instruments[selectedCoverage]) {
            // Get instruments for the selected satellite and coverage
            instruments = satRel.coverage_instruments[selectedCoverage];
        } else {
            // If no coverage selected, show all instruments for the satellite
            instruments = satRel.instruments;
        }
    } else if (!selectedSatellite) {
        // If no satellite selected, show all instruments
        instruments = satelliteRelationships.instruments;
    }
    
    // Filter out "Not Available" for display unless it's the only option
    let filteredInstruments = instruments.filter(i => i && i !== "Not Available");
    if (filteredInstruments.length === 0 && instruments.includes("Not Available")) {
        filteredInstruments = ["Not Available"];
    }
    
    // Add options to dropdown
    filteredInstruments.sort().forEach(instrument => {
        $select.append(`<option value="${instrument}">${instrument}</option>`);
    });
    
    // Restore previous selection if it exists in new data
    if (currentValue && filteredInstruments.includes(currentValue)) {
        $select.val(currentValue);
    } else if (filteredInstruments.length > 0) {
        // Select first instrument by default
        $select.val(filteredInstruments[0]);
    }
}

// Fetch satellite list and maintain current selections when changing days
function fetchSatellitesForDay(dateObj) {
    // Store current selections before updating the dropdowns
    const currentSatelliteId = $("#satellite_id").val();
    const currentCoverage = $("#coverage_select").val();
    const currentInstrument = $("#instrument_select").val();
    
    // Log for debugging
    console.log("Stored current selections:", {
        satellite: currentSatelliteId,
        coverage: currentCoverage,
        instrument: currentInstrument
    });
    
    let year = dateObj.getUTCFullYear();
    let month = (dateObj.getUTCMonth() + 1).toString().padStart(2, "0");
    let day = dateObj.getUTCDate().toString().padStart(2, "0");
    let dateStr = `${year}-${month}-${day}`;
    
    $.ajax({
        url: apiBasePath + '/satellites.py?date=' + dateStr,
        method: 'GET',
        dataType: 'json',
        success: function(response) {
            let $select = $("#satellite_id");
            $select.empty();
            $select.append('<option value="">All Satellites</option>');
            
            if(response.satellites && response.satellites.length > 0) {
                let satelliteFound = false;
                
                response.satellites
                    .sort((a, b) => a.id.localeCompare(b.id))
                    .forEach((sat) => {
                        const statusSymbol = sat.fileExists ? '✓' : '✗';
                        $select.append(`<option value="${sat.id}">${sat.id} ${statusSymbol}</option>`);
                        
                        // Check if our previously selected satellite is in the new list
                        if (currentSatelliteId && sat.id === currentSatelliteId) {
                            satelliteFound = true;
                        }
                    });
                
                // Update status display
                updateSatelliteStatus(response.satellites, response.baseDir);
                
                // Try to select the previously selected satellite if it exists in the new day
                if (satelliteFound) {
                    $select.val(currentSatelliteId);
                } else if (currentSatelliteId) {
                    console.log(`Previously selected satellite ${currentSatelliteId} not found in new day`);
                    // If the specific satellite isn't available, see if we should select "All Satellites"
                    if (currentSatelliteId === "") {
                        $select.val("");
                    } else {
                        // Select the first satellite as fallback
                        $select.val(response.satellites[0].id);
                    }
                } else {
                    // Default: select first satellite if no previous selection
                    $select.val(response.satellites[0].id);
                }
                
                // Update coverage dropdown based on relationships
                updateCoverageFromRelationships();
                
                // After coverage is updated, try to restore the previous coverage selection
                if (currentCoverage) {
                    // Check if the coverage option exists in the new dropdown
                    if ($("#coverage_select option[value='" + currentCoverage + "']").length) {
                        $("#coverage_select").val(currentCoverage);
                    }
                    
                    // Update instrument dropdown based on the new (or restored) coverage
                    updateInstrumentFromRelationships();
                    
                    // After instrument is updated, try to restore the previous instrument selection
                    if (currentInstrument) {
                        // Check if the instrument option exists in the new dropdown
                        if ($("#instrument_select option[value='" + currentInstrument + "']").length) {
                            $("#instrument_select").val(currentInstrument);
                        }
                    }
                }

                // Fetch data for the selected options
                if (autoLoadEnabled) {
                    fetchDataForDay(dateObj);
                } else {
                    hideLoading();
                    // Make sure the confirm button is visible and reset
                    $("#confirmButtonContainer").show();
                    resetConfirmButton();
                }
            } else {
                hideLoading();
                showError("No satellites available for the selected date.");
            }
        },
        error: function(jqXHR, textStatus, errorThrown) {
            hideLoading();
            console.error("Satellite API Error:", textStatus, errorThrown);
            showError("Error fetching satellite list. Please try again later.");
        }
    });
}

// Show/Hide loading indicators
function showLoading() {
    $("#loading").show();
    $("#errorMessage").hide();
    $("#chart").hide();
    $("#dataTable").hide();
}

function hideLoading() {
    $("#loading").hide();
    $("#chart").show();
    $("#dataTable").show();
}

function showError(message, details = "") {
    $("#errorMessage").html(`
        <strong>Error:</strong> ${message}
        ${details ? `<pre>${details}</pre>` : ""}
    `).show();
    $("#chart").hide();
    $("#dataTable").hide();
}

// Update debug info panel
function updateDebugInfo(data) {
    if (!data || data.length === 0) {
        $("#debugContent").html("<p>No data available</p>");
        return;
    }
    
    // Sample first data item
    const firstItem = data[0];
    const keys = Object.keys(firstItem);
    
    // Count of unique values for key fields
    const satelliteIDs = [...new Set(data.map(item => item.satellite_id || "undefined"))];
    const instruments = [...new Set(data.map(item => item.instrument || "undefined"))];
    const coverages = [...new Set(data.map(item => item.coverage || "undefined"))];
    
    let html = `
        <div class="mb-3">
            <strong>Data sample (first record):</strong>
            <pre>${JSON.stringify(firstItem, null, 2)}</pre>
        </div>
        <div class="mb-3">
            <strong>Available fields:</strong> ${keys.join(", ")}
        </div>
        <div class="mb-3">
            <strong>Unique satellite_id values (${satelliteIDs.length}):</strong> ${satelliteIDs.join(", ")}
        </div>
        <div class="mb-3">
            <strong>Unique instrument values (${instruments.length}):</strong> ${instruments.join(", ")}
        </div>
        <div class="mb-3">
            <strong>Unique coverage values (${coverages.length}):</strong> ${coverages.join(", ")}
        </div>
        <div class="mb-3">
            <strong>Total records:</strong> ${data.length}
        </div>
    `;
    
    $("#debugContent").html(html);
}

// Update satellite status display
function updateSatelliteStatus(satellites, baseDir) {
    const $status = $("#satelliteStatus");
    const $content = $("#satelliteStatusContent");
    const $dirPath = $("#directoryPath");
    
    $content.empty();
    satellites.forEach(sat => {
        const statusClass = sat.fileExists ? 'status-exists' : 'status-missing';
        const statusText = sat.fileExists ? 'File exists' : 'File missing';
        $content.append(`
            <div class="satellite-info">
                <span class="fw-bold">${sat.id}:</span> 
                <span class="${statusClass}">${statusText}</span>
            </div>
        `);
    });
    
    $dirPath.html(`<strong>Directory:</strong> ${baseDir}`);
    $status.show();
}

// Update coverage dropdown based on available data
function updateCoverageDropdown(data) {
    const coverages = [...new Set(data.map(item => item.coverage))]
        .filter(c => c && c !== "Not Available")
        .sort();
    
    const $select = $("#coverage_select");
    const currentValue = $select.val();
    
    $select.empty();
    $select.append('<option value="">All Coverages</option>');
    
    coverages.forEach(coverage => {
        $select.append(`<option value="${coverage}">${coverage}</option>`);
    });
    
    // Restore previous selection if it exists in new data
    if (currentValue && coverages.includes(currentValue)) {
        $select.val(currentValue);
    }
}

// Update instrument dropdown based on available data
function updateInstrumentDropdown(data) {
    const instruments = [...new Set(data.map(item => item.instrument))]
        .filter(i => i && i !== "Not Available")
        .sort();
    
    const $select = $("#instrument_select");
    const currentValue = $select.val();
    
    $select.empty();
    $select.append('<option value="">All Instruments</option>');
    
    instruments.forEach(instrument => {
        $select.append(`<option value="${instrument}">${instrument}</option>`);
    });
    
    // Restore previous selection if it exists in new data
    if (currentValue && instruments.includes(currentValue)) {
        $select.val(currentValue);
    } else if (instruments.length > 0) {
        // Select first instrument by default if no previous selection
        $select.val(instruments[0]);
    }
}

// Optimized API response handling with memory management
// Function 3: fetchDataForDay - Complete updated function
function fetchDataForDay(dateObj){
    let year = dateObj.getFullYear();
    let month = (dateObj.getMonth() + 1).toString().padStart(2, "0");
    let day = dateObj.getDate().toString().padStart(2, "0");
    let dateStr = `${year}-${month}-${day}`;
    
    // Get selected options
    let selectedSatellite = $("#satellite_id").val();
    let selectedCoverage = $("#coverage_select").val();
    let selectedInstrument = $("#instrument_select").val();
    
    // Use fixed interval (5T)
    let query = `start_date=${dateStr}&end_date=${dateStr}&start_hour=00:00&end_hour=23:59`;
    if (selectedSatellite) {
        query += `&satellite_id=${selectedSatellite}`;
    }
    if (selectedCoverage) {
        query += `&coverage=${selectedCoverage}`;
    }
    if (selectedInstrument) {
        query += `&instrument=${selectedInstrument}`;
    }
    
    console.log("Fetching data with query:", query);
    showLoading();
    
    // Cancel previous AJAX request if it exists
    if (window.currentAjaxRequest) {
        window.currentAjaxRequest.abort();
    }
    
    // Clear previous data to free memory
    if (window.globalData) {
        window.globalData = null;
    }
    
    // Use low-level XHR for better memory control
    window.currentAjaxRequest = $.ajax({
        url: apiBasePath + '/data.py?' + query,
        method: 'GET',
        dataType: 'json',
        success: function(response) {
            hideLoading();
            window.currentAjaxRequest = null;
            
            // Reset confirm button if it exists
            resetConfirmButton();
            
            try {
                if (response.data && response.data.length > 0) {
                    // Store in global variable for reuse without refetching
                    window.globalData = response.data;
                    
                    // Update debug information
                    updateDebugInfo(response.data);
                    
                    // Update dropdowns
                    updateCoverageDropdown(response.data);
                    updateInstrumentDropdown(response.data);
                    
                    // Apply filters - Use efficient filtering
                    let filteredData;
                    
                    if (selectedCoverage || selectedInstrument) {
                        filteredData = response.data.filter(item => {
                            let matchesCoverage = !selectedCoverage || item.coverage === selectedCoverage;
                            let matchesInstrument = !selectedInstrument || item.instrument === selectedInstrument;
                            return matchesCoverage && matchesInstrument;
                        });
                    } else {
                        filteredData = response.data;
                    }
                    
                    if (filteredData.length > 0) {
                        displayData(filteredData);
                    } else {
                        showError("No data available after applying filters.");
                    }
                } else {
                    showError("No data available for the selected period.");
                }
            } catch (e) {
                console.error("Error processing response:", e);
                showError("Error processing data: " + e.message, e.stack);
            }
        },
        error: function(jqXHR, textStatus, errorThrown) {
            hideLoading();
            window.currentAjaxRequest = null;
            
            // Reset confirm button if it exists
            resetConfirmButton();
            
            // Don't process if it was manually aborted
            if (textStatus === 'abort') {
                console.log("Request aborted");
                return;
            }
            
            const errorDetails = {
                url: this.url,
                status: jqXHR.status,
                statusText: jqXHR.statusText,
                responseText: jqXHR.responseText ? 
                    (jqXHR.responseText.length > 1000 ? 
                        jqXHR.responseText.substring(0, 1000) + '...' : 
                        jqXHR.responseText) : 
                    'No response text'
            };
            
            console.error("API Error:", errorDetails);
            
            const additionalDetails = {
                selectedSatellite: selectedSatellite,
                selectedCoverage: selectedCoverage,
                selectedInstrument: selectedInstrument,
                dateStr: dateStr
            };
            
            console.error("Error Details:", additionalDetails);
            
            // Display the error message with details on the website
            showError(
                "Error fetching data. Please see below for more details.",
                JSON.stringify(errorDetails, null, 2)
            );
        }
    });
}

// Update coverage dropdown based on available data - optimized
function updateCoverageDropdown(data) {
    // Use Set for faster unique value lookup
    const coverageSet = new Set();
    
    // Collect unique coverage values
    for (const item of data) {
        if (item.coverage && item.coverage !== "Not Available") {
            coverageSet.add(item.coverage);
        }
    }
    
    // Convert to sorted array
    const coverages = Array.from(coverageSet).sort();
    
    const $select = $("#coverage_select");
    const currentValue = $select.val();
    
    $select.empty();
    $select.append('<option value="">All Coverages</option>');
    
    // Build options HTML more efficiently
    const optionsHtml = coverages.map(coverage => 
        `<option value="${coverage}">${coverage}</option>`
    ).join('');
    
    $select.append(optionsHtml);
    
    // Restore previous selection if it exists in new data
    if (currentValue && coverageSet.has(currentValue)) {
        $select.val(currentValue);
    }
}

// Update instrument dropdown based on available data - optimized
function updateInstrumentDropdown(data) {
    // Use Set for faster unique value lookup
    const instrumentSet = new Set();
    
    // Collect unique instrument values
    for (const item of data) {
        if (item.instrument && item.instrument !== "Not Available") {
            instrumentSet.add(item.instrument);
        }
    }
    
    // Convert to sorted array
    const instruments = Array.from(instrumentSet).sort();
    
    const $select = $("#instrument_select");
    const currentValue = $select.val();
    
    $select.empty();
    $select.append('<option value="">All Instruments</option>');
    
    // Build options HTML more efficiently
    const optionsHtml = instruments.map(instrument => 
        `<option value="${instrument}">${instrument}</option>`
    ).join('');
    
    $select.append(optionsHtml);
    
    // Restore previous selection if it exists in new data
    if (currentValue && instrumentSet.has(currentValue)) {
        $select.val(currentValue);
    } else if (instruments.length > 0) {
        // Select first instrument by default if no previous selection
        $select.val(instruments[0]);
    }
}

// Update debug info panel - optimized for large datasets
function updateDebugInfo(data) {
    if (!data || data.length === 0) {
        $("#debugContent").html("<p>No data available</p>");
        return;
    }
    
    // Sample first data item
    const firstItem = data[0];
    const keys = Object.keys(firstItem);
    
    // Use Sets for faster unique value lookup
    const satelliteSet = new Set();
    const instrumentSet = new Set();
    const coverageSet = new Set();
    
    // Only process a reasonable number of records for statistics
    const maxSamples = Math.min(data.length, 1000);
    
    for (let i = 0; i < maxSamples; i++) {
        const item = data[i];
        satelliteSet.add(item.satellite_id || "undefined");
        instrumentSet.add(item.instrument || "undefined");
        coverageSet.add(item.coverage || "undefined");
    }
    
    // Check if we have more records than we sampled
    const samplingNote = data.length > maxSamples ? 
        `<p class="text-muted">(Statistics based on first ${maxSamples} of ${data.length} records)</p>` : '';
    
    let html = `
        <div class="mb-3">
            <strong>Data sample (first record):</strong>
            <pre>${JSON.stringify(firstItem, null, 2)}</pre>
        </div>
        <div class="mb-3">
            <strong>Available fields:</strong> ${keys.join(", ")}
        </div>
        <div class="mb-3">
            <strong>Unique satellite_id values (${satelliteSet.size}):</strong> 
            ${Array.from(satelliteSet).join(", ")}
        </div>
        <div class="mb-3">
            <strong>Unique instrument values (${instrumentSet.size}):</strong> 
            ${Array.from(instrumentSet).join(", ")}
        </div>
        <div class="mb-3">
            <strong>Unique coverage values (${coverageSet.size}):</strong> 
            ${Array.from(coverageSet).join(", ")}
        </div>
        <div class="mb-3">
            <strong>Total records:</strong> ${data.length}
        </div>
        ${samplingNote}
    `;
    
    $("#debugContent").html(html);
}

// Add a download button to the UI
function addDownloadButton() {
    // Check if the download button already exists
    if ($("#downloadButton").length === 0) {
        // Add the button after the chart or before the data table
        $("#dataTable").before(`
            <div class="d-flex justify-content-end mb-3">
                <button id="downloadButton" class="btn btn-primary">
                    <i class="bi bi-download me-1"></i> Download CSV
                </button>
            </div>
        `);
        
        // Add event listener to the button
        $("#downloadButton").on("click", function() {
            downloadCurrentDataAsCSV();
        });
    }
}

// Function to download current data as CSV
function downloadCurrentDataAsCSV() {
    // Check if we have data to download
    if (!window.globalData || window.globalData.length === 0) {
        alert("No data available to download.");
        return;
    }
    
    try {
        // Extract the selected filters for the filename
        const selectedSatellite = $("#satellite_id").val() || "All";
        const selectedCoverage = $("#coverage_select").val() || "All";
        const selectedInstrument = $("#instrument_select").val() || "All";
        const dateStr = $("#currentDateDisplay").text() || new Date().toISOString().split('T')[0];
        
        // Create filename
        const filename = `satellite_latency_${selectedSatellite}_${dateStr}.csv`;
        
        // Get the headers from the first data item
        const headers = Object.keys(window.globalData[0]);
        
        // Create CSV content
        let csvContent = headers.join(",") + "\n";
        
        // Add data rows
        window.globalData.forEach(row => {
            const values = headers.map(header => {
                const value = row[header];
                // Handle special cases (commas, quotes, etc.)
                if (value === null || value === undefined) {
                    return '';
                } else if (typeof value === 'string' && (value.includes(',') || value.includes('"'))) {
                    return `"${value.replace(/"/g, '""')}"`;
                } else {
                    return value;
                }
            });
            csvContent += values.join(",") + "\n";
        });
        
        // Create a blob and download link
        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        
        // Create a temporary link and click it
        const link = document.createElement("a");
        link.setAttribute("href", url);
        link.setAttribute("download", filename);
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        
        console.log(`Downloaded ${window.globalData.length} records to ${filename}`);
    } catch (error) {
        console.error("Error downloading CSV:", error);
        alert("Error creating CSV file. See console for details.");
    }
}

// Display chart and data table with optimizations for large datasets
function displayData(data, currentUTCDay, nextUTCDay) {
    if(!data || data.length === 0) {
        showError("No data available to display.");
        return;
    }

    try {
        // Create UTC day boundaries if not provided
        if (!currentUTCDay) {
            currentUTCDay = new Date(Date.UTC(
                currentDate.getUTCFullYear(),
                currentDate.getUTCMonth(),
                currentDate.getUTCDate()
            ));
            
            nextUTCDay = new Date(currentUTCDay);
            nextUTCDay.setUTCDate(nextUTCDay.getUTCDate() + 1);
        }

        // Debug logging
        console.log("Display data called with", data.length, "records");
        console.log("UTC day bounds:", currentUTCDay.toISOString(), "to", nextUTCDay.toISOString());
        
        addDownloadButton();
        
        // Check if dataset is too large and warn user
        const DATA_WARNING_THRESHOLD = 5000;
        if (data.length > DATA_WARNING_THRESHOLD) {
            console.warn(`Large dataset detected (${data.length} records). Downsampling to prevent browser performance issues.`);
            $("#dataWarning").html(`
                <div class="alert alert-warning">
                    <strong>Large dataset detected:</strong> ${data.length} records found. 
                    Downsampling applied to prevent performance issues.
                </div>
            `).show();
            
            // Downsample data for very large datasets
            data = downsampleData(data, 2000); // Limit to 2000 points
        } else {
            $("#dataWarning").hide();
        }
        
        // Normalize data for consistent display and type safety
        data = data.map(item => ({
            satellite_id: item.satellite_id || 'Unknown',
            instrument: item.instrument || 'Unknown',
            coverage: item.coverage || 'Unknown',
            ingest_source: item.ingest_source || 'Not Available',
            latency: typeof item.latency === 'string' ? parseFloat(item.latency) : item.latency,
            // Ensure start_time is a Date object (safely)
            start_time: ensureDate(item.start_time)
        }));
        
        // Double-check that all data is within the UTC day boundaries
        data = data.filter(item => 
            item.start_time >= currentUTCDay && 
            item.start_time < nextUTCDay && 
            !isNaN(item.latency) && 
            !isNaN(item.start_time.getTime())
        );
        
        console.log(`Final data points after strict UTC filtering: ${data.length}`);

        if (data.length === 0) {
            showError("No data available within the selected UTC day.");
            return;
        }
        
        // Sort data by time
        data.sort((a, b) => a.start_time - b.start_time);
        
        // Group by ingest_source - use Map for better performance with large datasets
        const ingestSourceMap = new Map();
        
        // Prepare data for each ingest source
        for (const item of data) {
            const source = item.ingest_source === "Not Available" ? "Unknown" : item.ingest_source;
            if (!ingestSourceMap.has(source)) {
                ingestSourceMap.set(source, {
                    x: [],
                    y: [],
                    source: source
                });
            }
            const sourceData = ingestSourceMap.get(source);
            sourceData.x.push(item.start_time);
            sourceData.y.push(item.latency);
        }

        // Convert Map to array of traces with explicit UTC time handling
        const ingestSources = Array.from(ingestSourceMap.keys());
        const traces = ingestSources.map(source => {
            const sourceData = ingestSourceMap.get(source);
            return {
                x: sourceData.x.map(date => date.toISOString()), // Force UTC interpretation with ISO string
                y: sourceData.y,
                type: 'scattergl', // Use WebGL for better performance with large datasets
                mode: 'lines+markers',
                name: source,
                line: { width: 2 },
                marker: { size: 4 } // Smaller markers for better performance
            };
        });

        const selectedSatellite = $("#satellite_id").val() || "All";
        const selectedInstrument = $("#instrument_select").val() || "All";
        const selectedCoverage = $("#coverage_select").val() || "All";

        // Create the multi-select dropdown with checkboxes if it doesn't exist
        if ($("#ingest-source-filter").length === 0) {
            $("#chart").before(`
                <div id="dataWarning" class="mb-3" style="display: none;"></div>
                <div id="ingest-source-filter" class="mb-4 p-3 bg-light rounded">
                    <div class="d-flex justify-content-between align-items-center mb-2">
                        <h5 class="m-0">Filter by Ingest Source</h5>
                        <div>
                            <button id="select-all-sources" class="btn btn-sm btn-outline-primary me-2">Select All</button>
                            <button id="deselect-all-sources" class="btn btn-sm btn-outline-secondary">Deselect All</button>
                        </div>
                    </div>
                    <div id="ingest-checkboxes" class="d-flex flex-wrap">
                        <!-- Checkboxes will be inserted here -->
                    </div>
                </div>
            `);

            // Add event handlers for select/deselect all buttons
            $("#select-all-sources").on("click", function() {
                $("#ingest-checkboxes input[type='checkbox']").prop("checked", true).trigger("change");
            });
            
            $("#deselect-all-sources").on("click", function() {
                $("#ingest-checkboxes input[type='checkbox']").prop("checked", false).trigger("change");
            });
        }

        // Update checkboxes - only create what's needed
        const $checkboxContainer = $("#ingest-checkboxes");
        $checkboxContainer.empty();
        
        // Create a unique ID for each checkbox based on the ingest source name
        ingestSources.forEach((source, index) => {
            const checkboxId = `ingest-source-${index}`;
            const sourceDisplayName = source.length > 30 ? source.substring(0, 30) + '...' : source;
            
            $checkboxContainer.append(`
                <div class="form-check me-4 mb-2">
                    <input class="form-check-input ingest-checkbox" type="checkbox" id="${checkboxId}" 
                           data-index="${index}" data-source="${source}" checked>
                    <label class="form-check-label" for="${checkboxId}" title="${source}">
                        ${sourceDisplayName}
                    </label>
                </div>
            `);
        });

        // Use event delegation for checkbox changes to avoid too many listeners
        $checkboxContainer.off("change").on("change", ".ingest-checkbox", function() {
            updateChartVisibility();
        });

        // Function to update chart visibility based on selected checkboxes
        function updateChartVisibility() {
            // Get checked indices using more efficient query
            const selectedIndices = new Set();
            document.querySelectorAll(".ingest-checkbox:checked").forEach(function(checkbox) {
                selectedIndices.add(parseInt(checkbox.getAttribute("data-index")));
            });
            
            // Create visibility array (more efficient than map)
            const visibility = traces.map((_, index) => 
                selectedIndices.has(index) ? true : "legendonly"
            );
            
            // Use efficient partial update rather than full re-render
            Plotly.restyle('chart', {
                visible: visibility
            });
        }

        // Determine min and max times in the data for proper range selection
        let minTime = new Date();
        let maxTime = new Date(0); // Initialize to epoch
        
        // More efficient min/max calculation
        for (const item of data) {
            const time = item.start_time;
            if (time < minTime) minTime = time;
            if (time > maxTime) maxTime = time;
        }
        
        // Format the date to display in the chart title
        const formattedDate = currentUTCDay.toISOString().split('T')[0];
        const dateStr = formattedDate;

        // Create comprehensive layout with proper UTC handling
        let layout = {
            title: {
                text: `Latency Data for ${selectedSatellite} - ${selectedInstrument} (${formattedDate} UTC)`,
                font: { size: 24 }
            },
            xaxis: {
                title: 'Start Time (UTC)',
                tickangle: -45,
                gridcolor: '#eee',
                type: 'date',
                // Force UTC interpretation for all dates
                hoverformat: '%Y-%m-%d %H:%M:%S UTC',
                tickformat: '%H:%M\n%b %d',
                // Enable UTC mode for all date handling
                tickformatstops: [{
                    dtickrange: [null, 86400000],
                    value: '%H:%M\n%b %d'
                }],
                // Set exact range to current UTC day boundaries with explicit UTC marker
                range: [
                    currentUTCDay.toISOString(),
                    nextUTCDay.toISOString()
                ],
                // Force fixed range mode
                autorange: false,
                fixedrange: false,
                
                // Configure range selector to maintain day boundaries
                rangeselector: {
                    buttons: [
                        {
                            count: 1,
                            label: '1h',
                            step: 'hour',
                            stepmode: 'backward'
                        },
                        {
                            count: 3,
                            label: '3h',
                            step: 'hour',
                            stepmode: 'backward'
                        },
                        {
                            count: 6,
                            label: '6h',
                            step: 'hour',
                            stepmode: 'backward'
                        },
                        {
                            count: 12,
                            label: '12h',
                            step: 'hour',
                            stepmode: 'backward'
                        },
                        {
                            count: 24,
                            step: 'hour',
                            label: 'All Day',
                            // When "All" is clicked, return to the full day view
                            stepmode: 'todate'
                        }
                    ],
                    x: 0.05,
                    y: 1.1,
                    bgcolor: '#f8f9fa',
                    bordercolor: '#dee2e6',
                    font: { size: 12 }
                },
                
                // Modify rangeslider to respect boundaries
                rangeslider: {
                    visible: true,
                    thickness: 0.05,
                    range: [
                        currentUTCDay.toISOString(),
                        nextUTCDay.toISOString()
                    ]
                }
            },
            yaxis: {
                title: 'Average Latency (seconds)',
                rangemode: 'tozero',
                gridcolor: '#eee'
            },
            margin: {
                l: 60,
                r: 100,
                t: 120, // Increased to accommodate the range selector
                b: 100  // Increased to accommodate the range slider
            },
            showlegend: true,
            legend: {
                title: { text: 'Ingest Source' },
                x: 1.05,
                y: 1,
                xanchor: 'left',
                yanchor: 'top'
            },
            height: 650, // Increased height to accommodate the range slider
            
            // Critical for maintaining our fixed range settings
            uirevision: dateStr, // Use the date string to reset when day changes
            hovermode: 'closest'
        };

        // Explicitly set Plotly configuration for UTC handling
        let config = {
            responsive: true,
            displayModeBar: true,
            displaylogo: false,
            modeBarButtonsToRemove: ['lasso2d', 'select2d'],
            toImageButtonOptions: {
                format: 'png',
                filename: `latency_${selectedSatellite}_${formattedDate}_UTC`
            },
            // Force UTC time interpretation
            locales: {
                'en-US': {
                    date: '%Y-%m-%d',
                    hours: '%H:%M',
                    months: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
                    shortMonths: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
                    days: ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'],
                    shortDays: ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
                }
            },
            locale: 'en-US'
        };

        // Use newPlot only on first render, otherwise update
        if (document.getElementById('chart').data) {
            Plotly.react('chart', traces, layout, config);
        } else {
            Plotly.newPlot('chart', traces, layout, config);
        }
        
        // Build data table more efficiently by calculating HTML once
        // Don't show all data in the table, limit to a reasonable number
        const MAX_TABLE_ROWS = 1000;
        const tableData = data.length > MAX_TABLE_ROWS 
            ? data.slice(0, MAX_TABLE_ROWS) 
            : data;
            
        let tableRows = '';
        for (const row of tableData) {
            // Format time as HH:MM:SS
            const timeString = row.start_time.toISOString().replace('T', ' ').replace('Z', '');
            const instrument = row.instrument === "Not Available" ? "Unknown" : row.instrument;
            const coverage = row.coverage === "Not Available" ? "Unknown" : row.coverage;
            const ingestSource = row.ingest_source === "Not Available" ? "Unknown" : row.ingest_source;
            
            tableRows += `
                <tr>
                    <td>${timeString}</td>
                    <td>${row.satellite_id}</td>
                    <td>${instrument}</td>
                    <td>${coverage}</td>
                    <td>${ingestSource}</td>
                    <td>${row.latency.toFixed(2)}</td>
                </tr>
            `;
        }

        const tableHtml = `
            <table class="table table-striped table-hover">
                <thead class="table-light">
                    <tr>
                        <th>Start Time (UTC)</th>
                        <th>Satellite ID</th>
                        <th>Instrument</th>
                        <th>Coverage</th>
                        <th>Ingest Source</th>
                        <th>Average Latency (s)</th>
                    </tr>
                </thead>
                <tbody>
                    ${tableRows}
                </tbody>
            </table>
            ${data.length > MAX_TABLE_ROWS ? 
              `<div class="alert alert-info">Showing ${MAX_TABLE_ROWS} of ${data.length} records</div>` : ''}
        `;
        
        $("#dataTable").html(tableHtml);

        // Add custom CSS for checkbox container
        if (!$("#ingest-filter-style").length) {
            $("head").append(`
                <style id="ingest-filter-style">
                    #ingest-checkboxes {
                        max-height: 200px;
                        overflow-y: auto;
                    }
                    .form-check-label {
                        white-space: nowrap;
                        overflow: hidden;
                        text-overflow: ellipsis;
                        max-width: 250px;
                        cursor: pointer;
                    }
                    /* Style for range selector buttons */
                    .plotly .rangeselector button {
                        font-weight: 600;
                        color: #495057;
                        background: white;
                        border: 1px solid #ced4da;
                        border-radius: 4px;
                        padding: 3px 8px;
                        margin-right: 5px;
                    }
                    .plotly .rangeselector button.active {
                        color: white;
                        background: #0d6efd;
                        border-color: #0d6efd;
                    }
                </style>
            `);
        }

    } catch (error) {
        console.error("Error displaying data:", error);
        showError("Error displaying data: " + error.message, error.stack);
    }
}

// Helper function to ensure a value is a valid Date object
function ensureDate(value) {
    if (value instanceof Date) {
        return value;
    }
    
    try {
        const date = new Date(value);
        // Check if date is valid
        return isNaN(date.getTime()) ? new Date() : date;
    } catch (e) {
        console.warn("Invalid date value:", value);
        return new Date(); // Return current date as fallback
    }
}



// Helper function to downsample data for large datasets
function downsampleData(data, targetPoints) {
    if (data.length <= targetPoints) {
        return data;
    }
    
    // Simple downsampling by taking evenly spaced points
    const skip = Math.ceil(data.length / targetPoints);
    const sampledData = [];
    
    for (let i = 0; i < data.length; i += skip) {
        sampledData.push(data[i]);
    }
    
    console.log(`Downsampled from ${data.length} to ${sampledData.length} points`);
    return sampledData;
}