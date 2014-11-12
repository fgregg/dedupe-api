(function(){
    get_cluster();
    $('.mark-entity').on('click', function(e){
        e.preventDefault();
        var entity_id = $('#group-display').data('group');
        var action = $(this).attr('id').split('_')[0];
        $.getJSON(mark_cluster_url, {'entity_id': entity_id, 'action': action}, function(resp){
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
                $('#group-display').data('group', resp.entity_id);
                $.each(resp.objects, function(i, item){
                    if (i == 0){
                        head += '<tr>';
                        $.each(item, function(k,v){
                            head += '<th>' + k + '</th>';
                        });
                        head += '</tr>';
                    }
                    body += '<tr>';
                    $.each(item, function(k,v){
                        body += '<td>' + v + '</td>';
                    });
                    body += '</tr>';
                });
                $('#confidence').html('(confidence of ' + resp.confidence + ')');
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

