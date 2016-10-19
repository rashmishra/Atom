db.apps_flyer.drop();
db.app_flier.find().forEach(function(doc){
    var newdoc = JSON.parse(doc.payload);

    var event_dateString = newdoc.event_time,
    dateTimeParts = event_dateString.split(' '),
    timeParts = dateTimeParts[1].split(':'),
    dateParts = dateTimeParts[0].split('-'),
    date;
    v_event_time_epoch = new Date(dateParts[0], parseInt(dateParts[1], 10) - 1, dateParts[2], timeParts[0], timeParts[1], timeParts[2]);
    newdoc.event_time_epoch = v_event_time_epoch.getTime();
    // newdoc.event_time = v_event_time_epoch;

    if (newdoc.download_time != null ){
		var download_dateString = newdoc.download_time,
	    dateTimeParts = download_dateString.split(' '),
	    timeParts = dateTimeParts[1].split(':'),
	    dateParts = dateTimeParts[0].split('-'),
	    date;
	    v_download_time_epoch = new Date(dateParts[0], parseInt(dateParts[1], 10) - 1, dateParts[2], timeParts[0], timeParts[1], timeParts[2]);
	    newdoc.download_time_epoch = v_download_time_epoch.getTime();
	    // newdoc.download_time = v_download_time_epoch;
	}
	else {

		newdoc.download_time_epoch = 0;

	}

    if (newdoc.install_time != null ){
		var install_dateString = newdoc.install_time,
	    dateTimeParts = install_dateString.split(' '),
	    timeParts = dateTimeParts[1].split(':'),
	    dateParts = dateTimeParts[0].split('-'),
	    date;
	    v_install_time_epoch = new Date(dateParts[0], parseInt(dateParts[1], 10) - 1, dateParts[2], timeParts[0], timeParts[1], timeParts[2]);
	    newdoc.install_time_epoch = v_install_time_epoch.getTime();
	    // newdoc.install_time = v_install_time_epoch;
	}
	else {

		newdoc.install_time_epoch = 0;

	}

    if (newdoc.click_time != null ){
		var click_dateString = newdoc.click_time,
	    dateTimeParts = click_dateString.split(' '),
	    timeParts = dateTimeParts[1].split(':'),
	    dateParts = dateTimeParts[0].split('-'),
	    date;
	    v_click_time_epoch = new Date(dateParts[0], parseInt(dateParts[1], 10) - 1, dateParts[2], timeParts[0], timeParts[1], timeParts[2]);
	    newdoc.click_time_epoch = v_click_time_epoch.getTime();
	    // newdoc.click_time = v_click_time_epoch;
	}
	else {

		newdoc.click_time_epoch = 0;

	}

    db.apps_flyer.save(newdoc);
})

db.apps_flyer.createIndex({"event_time_epoch": 1})