(function(){
    get_cluster();
    $('.mark-entity').on('click', function(e){
        e.preventDefault();
        var entity_id = $('#group-display').data('entity_id');
        var match_ids = []
        var distinct_ids = []
        $.each($('input[type="checkbox"]'), function(i, inp){
            var record_id = $(inp).parent().data('record_id')
            if($(inp).is(':checked')){
                match_ids.push(record_id)
            } else {
                distinct_ids.push(record_id)
            }
        })
        var params = {
            'entity_id': entity_id,
            'match_ids': match_ids.join(','),
            'distinct_ids': distinct_ids.join(',')
        }
        $.getJSON(mark_cluster_url, params, function(resp){
            get_cluster();
        })
    })
    $('.accept-all').on('click', function(e){
        e.preventDefault();
        $.getJSON(mark_all_clusters_url, {'action': 'yes'}, function(resp){
            $('#group-display').html("<h3>" + resp['message'] + "</h3>")
            $('#review-buttons').hide()
            $('#counter').parent().hide()
        })
    })
    function get_cluster(){
        $('#group-display').spin('large');
        $.getJSON(get_cluster_url, {}, function(resp){
            $('#group-display').spin(false);
            if (resp.objects.length > 0){
                $('#counter').html('<p>' + resp.review_remainder + ' out of ' + resp.total_clusters + ' left to review</p>')
                var head = '';
                var body = '';
                $('#group-display').data('entity_id', resp.entity_id);
                $.each(resp.objects, function(i, item){
                    if (i == 0){
                        head += '<tr><th></th>';
                        $.each(item, function(k,v){
                            if (k != 'record_id'){
                                head += '<th>' + k + '</th>';
                            }
                        });
                        head += '</tr>';
                    }
                    body += '<tr>';
                    body += '<td data-record_id="' + item.record_id + '">';
                    body += '<input checked type="checkbox" /></td>'
                    $.each(item, function(k,v){
                        if (k != 'record_id'){
                            body += '<td>' + v + '</td>';
                        }
                    });
                    body += '</tr>';
                });
                $('#group-display thead').html(head);
                $('#group-display tbody').html(body);
            } else {
                $('#group-display').html("<h3>You're done!</h3>")
                $('#review-buttons').hide()
                $('#counter').parent().hide()
            }
        })
    }
})();

